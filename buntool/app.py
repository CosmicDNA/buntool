import csv
import logging
import os
import shutil
import tempfile
import textwrap
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from flask import Flask, current_app, jsonify, render_template, request, send_file
from waitress import serve
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from buntool import bundle

#import boto3

# def upload_to_s3(file_path, s3_key):
#     s3.upload_file(file_path, bucket_name, s3_key)
#     return f"s3://{bucket_name}/{s3_key}"

# app = Flask(__name__) # Will be created by the app factory

# Constants
MAX_FILENAME_LENGTH = 100
MIN_CSV_COLUMNS_FOR_SECTION_CHECK = 4

@dataclass
class RequestContext:
    """Holds context information for a bundle creation request."""

    session_id: str
    user_agent: str
    timestamp: str
    temp_dir: Path

def is_running_in_lambda():
    return 'AWS_LAMBDA_FUNCTION_NAME' in os.environ  # seems to work?


def strtobool(value: str) -> bool:
    value = value.lower()
    return value in ("y", "yes", "on", "1", "true", "t", "True", "enabled")

def get_output_filename(bundle_title: str, case_name: str, timestamp: str, fallback="Bundle"):
    # Takes in the bundle title, case name, and a timestamp.
    # purpose is to guard against extra-long filenames.
    # Returns the output filename picked among many fallback options.
    output_file = f"{bundle_title}_{case_name}_{timestamp}.pdf"
    if len(output_file) > MAX_FILENAME_LENGTH:
        # take first 20 chars of bundle title and add case name:
        output_file = f"{bundle_title[:20]}_{case_name}_{timestamp}.pdf"
    if len(output_file) > MAX_FILENAME_LENGTH:
        # take first 20 chars of bundle title, first 20 chars of case name, and add timestamp:
        output_file = f"{bundle_title[:30]}_{case_name[:30]}_{timestamp}.pdf"
    if len(output_file) > MAX_FILENAME_LENGTH:
        # this should never happen:
        output_file = f"{fallback}_{timestamp}.pdf"
    if len(output_file) > MAX_FILENAME_LENGTH:
        # this should doubly never happen:
        output_file = f"{timestamp}.pdf"
    return output_file

def synchronise_csv_index(uploaded_csv_path, filename_mappings):
    # takes the path of the uploaded csv file and a dictionary of filename mappings (due to sanitising filenames of uploads).
    # creates a new csv file with the same structure as the original, but with the filenames replaced with secure versions.
    # returns the path of the new csv file.
    logger = current_app.logger
    sanitised_filenames_csv_path = Path(str(uploaded_csv_path).replace('index_', 'securefilenames_index_'))
    logger.info(f"secure_csv_path: {sanitised_filenames_csv_path}")
    try:
        with Path(uploaded_csv_path).open(newline='', encoding='utf-8') as infile, \
            Path(sanitised_filenames_csv_path).open('w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            logger.debug("Reading input CSV:")

            for row in reader:
                logger.debug(f"Processing row: {row}")
                try:
                    if row and (row[0] == 'Filename' or (len(row) >= MIN_CSV_COLUMNS_FOR_SECTION_CHECK and row[3] == '1')):
                        logger.debug("..Found header or section marker row")
                        writer.writerow(row)
                        continue

                    original_upload_filename = row[0]
                    secure_name = filename_mappings.get(original_upload_filename)
                    if secure_name is None:
                        secure_name = secure_filename(original_upload_filename)

                    row[0] = secure_name
                    writer.writerow(row)
                    logger.debug(f"..Wrote processed file row: {row}")
                except Exception:
                    logger.exception(f"..Error processing row {row}")
                    raise

    except Exception:
        logger.exception("..Error in save_csv_index")
        raise

    logger.info(f"..saved csv index as {sanitised_filenames_csv_path}")
    return sanitised_filenames_csv_path

def _get_bundle_config_from_form(form: dict, context: RequestContext, logs_dir: Path):
    """Extracts bundle configuration from the request form."""
    bundle_title = form.get('bundle_title', 'Bundle') if form.get('bundle_title') else 'Bundle'
    case_name = form.get('case_name')
    claim_no = form.get('claim_no')
    case_details = [bundle_title, claim_no, case_name]

    return bundle.BundleConfig(
        timestamp=context.timestamp,
        case_details=case_details,
        csv_string=None,
        confidential_bool=form.get('confidential_bool'),
        zip_bool=True,  # option not implemented for GUI control.
        session_id=context.session_id,
        user_agent=context.user_agent,
        page_num_align=form.get('page_num_align'),
        index_font=form.get('index_font'),
        footer_font=form.get('footer_font'),
        page_num_style=form.get('page_num_style'),
        footer_prefix=form.get('footer_prefix'),
        date_setting=form.get('date_setting'),
        roman_for_preface=strtobool(form.get('roman_for_preface', 'false')),
        temp_dir=context.temp_dir,
        logs_dir=logs_dir,
        bookmark_setting=form.get('bookmark_setting', 'tab-title')
    )

def save_uploaded_file(file: FileStorage, directory, filename=None):
    """Saves a file from a request to a specified directory."""
    if not (file and file.filename):
        return None
    name_to_secure = filename if filename else file.filename
    secure_name = secure_filename(name_to_secure)
    filepath = Path(directory) / secure_name
    file.save(filepath)
    current_app.logger.debug(f"Saved file: {filepath}")
    return filepath

def _save_and_get_path(file: FileStorage, temp_dir: Path, secure_name: str):
    """Saves a single file and returns its path if successful, otherwise None."""
    saved_path = save_uploaded_file(file, temp_dir, secure_name)
    if saved_path and saved_path.exists():
        current_app.logger.info(f"..File saved to: {saved_path}")
        return saved_path
    current_app.logger.error(f"File not found or failed to save at path: {saved_path}")
    return None

def _process_uploaded_files(files: list[FileStorage], temp_dir: Path):
    """Saves uploaded files and creates filename mappings."""
    # Create a mapping of original filenames to their secure versions.
    filename_mappings = {
        file.filename: secure_filename(file.filename)
        for file in files if file.filename
    }

    # Save files and collect their paths using a list comprehension.
    input_files = [
        saved_path
        for file in files if file.filename and (saved_path := _save_and_get_path(file, temp_dir, filename_mappings[file.filename]))
    ]

    return input_files, filename_mappings

def _handle_coversheet_upload(request_files: dict[str, FileStorage], temp_dir: Path, session_id, timestamp: str):
    """Handles the coversheet upload and returns its secure filename."""
    if 'coversheet' in request_files and request_files['coversheet'].filename != '':
        current_app.logger.debug("Coversheet found in form submission")
        cover_file = request_files['coversheet']
        secure_coversheet_filename = f'coversheet_{session_id}_{timestamp}.pdf'
        coversheet_filepath = save_uploaded_file(cover_file, temp_dir, secure_coversheet_filename)
        current_app.logger.debug(f"Coversheet path: {coversheet_filepath}")
        return secure_coversheet_filename
    return None

def _handle_csv_index_upload(request_files: dict[str, FileStorage], temp_dir: Path, session_id, timestamp: str, filename_mappings):
    """Handles CSV index upload, sanitization, and returns the path to the sanitized CSV."""
    if 'csv_index' in request_files and request_files['csv_index'].filename:
        csv_file = request_files['csv_index']
        secure_csv_filename = f'index_{session_id}_{timestamp}.csv'
        saved_csv_path = save_uploaded_file(csv_file, temp_dir, secure_csv_filename)

        if not saved_csv_path or not Path(saved_csv_path).exists():
            msg = f"Index data did not upload correctly. Session code: {session_id}"
            current_app.logger.exception(f"CSV file not found or failed to save at path: {saved_csv_path}")
            # We return a tuple (error_response, None) to be handled by the caller
            return jsonify({"status": "error", "message": msg}), 400, None

        sanitised_csv_path = synchronise_csv_index(saved_csv_path, filename_mappings)
        return None, None, sanitised_csv_path

    return None, None, None

def _build_and_respond(received_output_file, zip_file_path, session_id):
    """Validates bundle artifacts, copies them to the final directory, and creates a success response."""
    bundles_dir = Path(tempfile.gettempdir()) / 'buntool' / 'bundles'

    if not (received_output_file and Path(received_output_file).exists()):
        msg = f"Error preparing PDF file for download. Session code: {session_id}"
        current_app.logger.exception(f"PDF file not found at: {received_output_file}")
        return jsonify({"status": "error", "message": msg}), 500

    final_output_path = bundles_dir / Path(received_output_file).name
    shutil.copy2(received_output_file, final_output_path)
    current_app.logger.debug(f"Copied final PDF to: {final_output_path}")

    final_zip_path_str = None
    zip_path = Path(zip_file_path)
    if zip_file_path and zip_path.exists():
        final_zip_path = bundles_dir / zip_path.name
        shutil.copy2(zip_file_path, final_zip_path)
        final_zip_path_str = str(final_zip_path)
        current_app.logger.debug(f"Copied final ZIP to: {final_zip_path}")
    else:
        current_app.logger.warning(f"ZIP file not found at: {zip_file_path}. Continuing without zip.")

    return jsonify({
        "status": "success",
        "message": "Bundle created successfully!",
        "bundle_path": str(final_output_path),
        "zip_path": final_zip_path_str
    })


# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/create_bundle', methods=['GET', 'POST'])
def create_bundle():
    if request.method == 'GET':
        return render_template('index.html')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_id = str(uuid.uuid4())[:8]
    user_agent = request.headers.get('User-Agent') or ""
    current_app.logger.debug("******************APP HEARS A CALL******************")
    current_app.logger.debug(f"New session ID: {session_id} {user_agent}")

    if 'files' not in request.files:
        current_app.logger.exception("Cannot create bundle: No files found in form submission")
        return jsonify({"status": "error", "message": "No files found. Please add files and try again."}), 400

    logs_dir = Path(tempfile.gettempdir()) / 'logs' if is_running_in_lambda() else Path('logs')

    session_file_handler = None
    try:
        base_dir = tempfile.gettempdir() if is_running_in_lambda() else '.'
        temp_dir = Path(base_dir) / 'tempfiles' / session_id
        temp_dir.mkdir(parents=True, exist_ok=True)
        current_app.logger.debug(f"Temporary directory created: {temp_dir}")

        logs_path = logs_dir / f'buntool_{session_id}.log'
        session_file_handler = logging.FileHandler(logs_path)
        log_format = '%(asctime)s-%(levelname)s-[APP]: %(message)s'
        file_formatter = logging.Formatter(log_format)
        session_file_handler.setLevel(logging.DEBUG)
        session_file_handler.setFormatter(file_formatter)
        current_app.logger.addHandler(session_file_handler)

        context = RequestContext(
            session_id=session_id,
            user_agent=user_agent,
            timestamp=timestamp,
            temp_dir=temp_dir
        )

        bundle_config = _get_bundle_config_from_form(request.form, context, logs_dir)
        output_file = get_output_filename(bundle_config.case_details[0], bundle_config.case_details[1], timestamp, \
                                          bundle_config.footer_prefix or "Bundle")
        current_app.logger.debug(f"generated output filename: {output_file}")

        files = request.files.getlist('files')
        total_size = sum(f.content_length for f in files if f.content_length is not None)
        if total_size > current_app.config['MAX_CONTENT_LENGTH']:
            msg = f"Total size of files exceeds maximum allowed size: {total_size} > {current_app.config['MAX_CONTENT_LENGTH']}"
            current_app.logger.exception(msg)
            return jsonify({"status": "error", "message": msg}), 400

        input_files, filename_mappings = _process_uploaded_files(files, temp_dir)

        secure_coversheet_filename = _handle_coversheet_upload(request.files, temp_dir, session_id, timestamp)

        error_response, status_code, sanitised_filenames_index_csv = _handle_csv_index_upload(
            request.files, temp_dir, session_id, timestamp, filename_mappings
        )
        if error_response:
            # If status_code is None, default to 400
            return error_response if status_code is None else (error_response, status_code)

        log_msg = f"""
            Calling buntool.create_bundle with params:
            ....input_files: {input_files}
            ....output_file: {output_file}
            ....secure_coversheet_filename: {secure_coversheet_filename}
            ....sanitised_filenames_index_csv: {sanitised_filenames_index_csv}
            ....bundle_config elements: {bundle_config.__dict__}"""
        current_app.logger.info(textwrap.dedent(log_msg))

        received_output_file, zip_file_path = bundle.create_bundle(
            input_files, output_file, secure_coversheet_filename, sanitised_filenames_index_csv, bundle_config
        )

        return _build_and_respond(received_output_file, zip_file_path, session_id)

    except Exception:
        current_app.logger.exception("Fatal Error in processing bundle")
        return jsonify({"status": "error", "message": f"Fatal error in creating bundle. Session code: {session_id}"}), 500

    finally:
        if session_file_handler and session_file_handler in current_app.logger.handlers:
            current_app.logger.removeHandler(session_file_handler)

# @app.route('/download/bundle', methods=['GET'])
def download_bundle():
    bundle_path = request.args.get('path')
    if not bundle_path:
        return jsonify({"status": "error", "message": "Download Error: Bundle download path could not be found."}), 400

    absolute_path = Path(bundle_path).resolve()
    if not absolute_path.exists():
        return jsonify(
            {"status": "error", "message": "Download Error: bundle does not exist in expected location."}), 404

    return send_file(absolute_path, as_attachment=True)

# @app.route('/download/zip', methods=['GET'])
def download_zip():
    zip_path = request.args.get('path')
    if not zip_path:
        return jsonify({"status": "error", "message": "Download Error: Zip download path could not be found."}), 400

    absolute_path = Path(zip_path).resolve()
    if not absolute_path.exists():
        return jsonify({"status": "error", "message": "Download Error: zip does not exist in expected location."}), 404

    return send_file(absolute_path, as_attachment=True)

def create_app():
    """Entry point for running the Flask application."""
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # file size limit in MB
    app.logger.setLevel(logging.DEBUG)

    logs_dir = Path(tempfile.gettempdir()) / 'logs' if is_running_in_lambda() else Path('logs')
    logs_dir.mkdir(parents=True, exist_ok=True)

    bundles_dir = Path(tempfile.gettempdir()) / 'buntool' / 'bundles'
    bundles_dir.mkdir(parents=True, exist_ok=True)

    with app.app_context():
        # Import and register routes within the app context
        @app.route('/')
        def index():
            return render_template('index.html')

        # Register other routes
        app.add_url_rule('/create_bundle', view_func=create_bundle, methods=['GET', 'POST'])
        app.add_url_rule('/download/bundle', view_func=download_bundle, methods=['GET'])
        app.add_url_rule('/download/zip', view_func=download_zip, methods=['GET'])

        # Replace all direct `app.logger` calls with `current_app.logger` or pass logger around

    # s3 = boto3.client('s3')
    # bucket_name = os.environ.get('s3_bucket', 'your-default-bucket')
    return app

def main():
    """Creates and runs the Flask application."""
    created_app = create_app()
    created_app.logger.info("buntool starting...")
    created_app.logger.debug("APP - Server started on port 7001.")
    serve(created_app, host='0.0.0.0', port=7001, threads=4, connection_limit=100, channel_timeout=120)


if __name__ == '__main__':
    main()
