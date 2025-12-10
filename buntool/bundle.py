# TODO
##############################################
##  BUGS
##############################################
# GENERAL
# - [x] make sure temp files delete
# - [x] add cron job to server to cleanup every few mins
# - [x] draft docx indexing
# - [x] responsive index overflows at some breakpoints
# - [ ] Possible niggle: handling of filenames with multiple `.` characters in names, or none of them. Is the code depending too much on there being
# an extension to the file at all?
##############################################
##  ROADMAP
##############################################
# Technical improvements
#   - [ ] General error handling in functions of app.py (file saving, dir creation, csv reading/writing)
#   - [ ] Validation of all strings passed through frontend
#   - [ ] validation of csv data passed from frontend, check headers and columns.
# Features
#   - [ ] Add ability to offset page numbers (start at N)
#   - [ ] Convenience for sections: Add section header, spawn upload area for that section, helps to organise files
#   - [ ] Add a write-metadata function: https://pypdf.readthedocs.io/en/stable/user/metadata.html
#   - [ ] ability to reload state (via zip import).
#       This would require --
#       - [ ] save option state (as json?)
#       - [ ] save csv
#       - [ ] save input files
#       - [ ] allow upload of zip which is then parsed out into options/csv/inputfiles
#       - [ ] the data structure point above will help with this, because then it just becomes a matter of setting variables from the lines of the
# file.
# PDF manipulation
import argparse
import csv
import functools
import logging
import os

# General
import re
import shutil
import textwrap
import zipfile
from datetime import datetime
from itertools import count
from pathlib import Path
from typing import NamedTuple

import pdfplumber
import reportlab.rl_config
from colorlog import ColoredFormatter
from pdfplumber.pdf import PDF
from pikepdf import OutlineItem, Pdf
from pikepdf._core import Page
from pypdf import PdfReader, PdfWriter
from pypdf.annotations import Link
from pypdf.generic import DictionaryObject as Dictionary
from pypdf.generic import Fit
from pypdf.generic import NameObject as Name

# reportlab stuff
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, StyleSheet1, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Flowable,
    Frame,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.rl_config import defaultPageSize
from werkzeug.utils import secure_filename

from buntool.bundle_config import BundleConfig, BundleConfigParams

# custom
from buntool.makedocxindex import DocxConfig, create_toc_docx
from buntool.textwrap_custom import dedent_and_log

# Set globals
PAGE_HEIGHT = defaultPageSize[1]
PAGE_WIDTH = defaultPageSize[0]  # reportlab page sizes used in more than one function

# Constants
MIN_CSV_COLUMNS_WITH_SECTION = 4
MIN_CSV_COLUMNS_NO_SECTION = 3
MAX_TITLE_LENGTH_FOR_HYPERLINK_SEARCH = 30
MIN_TOC_ENTRY_FIELDS = 3

bundle_logger = logging.getLogger("bundle_logger")


def configure_logger(bundle_config, session_id=None):
    """Configure a logger for the bundling process.

    where session_id is an 8-digit hex number.
    Since the temp files are deleted in production,
    logs are to be stored in a seprate file /tmp/logs.
    """
    logs_dir = bundle_config.logs_dir if bundle_config else "logs"

    if not Path(logs_dir).exists():
        Path(logs_dir).mkdir(parents=True)
    # Configure logging
    logger = logging.getLogger("bundle_logger")

    # Clear existing handlers to prevent duplicate logs on subsequent runs
    if bundle_logger.hasHandlers():
        bundle_logger.handlers.clear()

    bundle_logger.setLevel(logging.DEBUG)
    bundle_logger.propagate = False
    formatter = logging.Formatter("%(asctime)s-%(levelname)s-[BUN]: %(message)s")
    color_formatter = ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - [BUN]: %(message)s',
        log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'},
        reset=True
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    bundle_logger.addHandler(console_handler)

    if not session_id:
        session_id = datetime.now().strftime("%Y%m%d%H%M%S")  # fallback
    # logs path = buntool_timestamp.log:
    logs_path = Path(logs_dir) / f"buntool_{session_id}.log"
    session_file_handler = logging.FileHandler(logs_path)
    session_file_handler.setLevel(logging.DEBUG)
    session_file_handler.setFormatter(formatter)
    logger.addHandler(session_file_handler)
    return bundle_logger


def _try_delete_file(file_path):
    """Attempt to delete a file and log the outcome. Return the path if deletion fails, otherwise None."""
    if not file_path:
        return None  # Don't attempt to delete a None or empty path
    try:
        path_obj = Path(file_path)
        bundle_logger.debug(f"[CB]..Attempting to delete: {path_obj}")
        path_obj.unlink(missing_ok=True)
        if path_obj.exists():  # Check if deletion was successful
            bundle_logger.warning(f"[CB]....Could not delete {path_obj}. It may be locked by another process.")
            return str(path_obj)
    except Exception:
        bundle_logger.exception(f"[CB]....An unexpected error occurred while trying to delete {file_path}.")
        return file_path
    else:
        return None  # Deletion successful or file was already gone


def remove_temporary_files(list_of_temp_files):
    """Run at the end of the bundle process.

    Iterates through a list of temporary files, attempts to delete each, and returns a list of files that could not be deleted.
    """
    bundle_logger.debug(f"[CB]Cleaning up temporary files: {list_of_temp_files}")
    remaining_files = [result for file in list_of_temp_files if (result := _try_delete_file(file)) is not None]

    if remaining_files:
        bundle_logger.info(f"[CB]..Remaining temporary files (will be deleted on next system flush): {remaining_files}")
    else:
        bundle_logger.info("[CB]..All temporary files deleted successfully.")

    return remaining_files


def sanitise_latex(text):
    """Homebrew LaTeX sanitiser.

    Potential alternative available at: https://pythonhosted.org/latex/
    which has an escape_latex function.

    However, this is entirely unused in production: the LaTeX functionality
    has been ported by ReportLab, which is more portable for deployment on AWS.
    The LaTeX functions are maintained because they work well, look good, and I
    sometimes prefer them for self-hosted use.

    There's simple way of to enable LaTeX indexing. To make it work, replace calls
    to reportlab style functions with calls to LaTeX functions, and alter the
    values in the frontend 'index font' and 'footer font' form fields (in
    buntool.js) to reference the expected font names which are used by LaTeX.
    """
    replacements = {
        "_": "\\_",
        "$": "\\$",
        "%": "\\%",
        "#": "\\#",
        "{": "\\{",
        "&": "\\&",
        "}": "\\}",
        "[": "{[}",
        "]": "{]}",
        '"': "{''}",
        "|": "\\textbar{}",
        "\\": "\\textbackslash{}",
        "~": "\\textasciitilde{}",
        "<": "\\textless{}",
        ">": "\\textgreater{}",
        "^": "\\textasciicircum{}",
        "`": "{}`",
        "\n": "\\\\",
    }

    # Remove emojis and other non-ASCII characters  (ascii list from space  0x20 onwards)
    text = re.sub(r"[^\x20-\x7F]+", "", text)

    # replace awkward ascii characters with LaTeX commands:
    sanitised_text = "".join(replacements.get(c, c) for c in text)
    bundle_logger.debug(f"[SL].... Sanitised input '{text}' for LaTeX output '{sanitised_text}'")
    return sanitised_text


def parse_the_date(date, bundle_config):
    """Take a date input in YYYY-MM-DD format and format it.

    formats it according to user preferences from the following
    styles depending on state of date_setting:
    - YYYY-MM-DD
    - DD-MM-YYYY
    - MM-DD-YYYY
    - uk_longdate
    - us_longdate
    - uk_abbreviated_date
    - us_abbreviated_date
    or if setting is hide_date, don't do anything
    """
    if bundle_config.date_setting == "hide_date":
        return date
    # check if date matches the expected format
    if not re.match(r"\d{4}-\d{2}-\d{2}", date):
        bundle_logger.error(f"[PTD] Error: Date does not match expected format: {date}")
        return date
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d")

        formats = {
            "YYYY-MM-DD": "%Y-%m-%d",
            "DD-MM-YYYY": "%d/%m/%Y",
            "MM-DD-YYYY": "%m/%d/%Y",
            "uk_longdate": "%d %B %Y",
            "us_longdate": "%B %d, %Y",
            "uk_abbreviated_date": "%d %b %Y",
            "us_abbreviated_date": "%b %d, %Y",
        }

        return parsed_date.strftime(formats[bundle_config.date_setting])
    except KeyError:
        bundle_logger.exception(f"[PTD] Error: Unknown date setting: {bundle_config.date_setting}")
        return date


def load_index_data(csv_index, bundle_config):
    """Ingest a CSV of table-of-contents entries and return a dictionary.

    a dictionary of the data (in the create bundle function, saved as
    index_data). The resulting dictionary is the template for the whole
    bundle creation.
    The CSV is typically generated by the frontend and is expected to be
    properly formatted as follows:
        Headings:
                filename, userdefined_title, date, section
                where 'section' is a section-marker flag.
        for normal input files:
                [filename, title, date, 0]
        for section breaks:
                [SECTION, section_name,,1]
    There are some fallbacks in place in case the data is missing, but
     this should not happen. They are there mainly for testing purposes
     when using the code via CLI.
    """
    index_data = {}
    bundle_logger.debug(f"[LID]Loading index data from {csv_index}")
    with Path(csv_index).open(newline="") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            if len(row) >= MIN_CSV_COLUMNS_WITH_SECTION:
                filename, userdefined_title, raw_date, section = row
                formatted_date = parse_the_date(raw_date, bundle_config)
                # Store filename as provided by frontend
                index_data[filename] = (userdefined_title, formatted_date, section)
            elif len(row) >= MIN_CSV_COLUMNS_NO_SECTION:
                filename, userdefined_title, raw_date = row
                formatted_date = parse_the_date(raw_date, bundle_config)
                index_data[filename] = (userdefined_title, formatted_date, "")
            else:
                filename, userdefined_title = row
                bundle_logger.debug(f"Reading file entry: |{filename}|")
                index_data[filename] = (userdefined_title, "", "")
    bundle_logger.debug(f"[LID]..Loaded index data with {len(index_data)} entries:")
    for k, v in index_data.items():
        bundle_logger.debug(f"[LID]....Key: |{k}| -> Value: {v}")
    return index_data


def get_pdf_creation_date(file):
    """Extracts the creation date from a PDF file.

    This is purely a fallback function in case the
    user-supplied (or frontend-supplied) information is missing a date.
    """
    try:
        with Pdf.open(file) as pdf:
            creation_date = pdf.docinfo.get("/CreationDate", None)
            if creation_date:
                # Convert to string if it's a pikepdf.String object
                creation_date_str = str(creation_date)
                # Extract date in the format D:YYYYMMDDHHmmSS
                date_str = creation_date_str[2:10]
                date_obj = datetime.strptime(date_str, "%Y%m%d")
                return date_obj.strftime("%d.%m.%Y")
    except Exception:
        bundle_logger.exception(f"[GPCD]Error extracting creation date from {file}")
        creation_date = None
        return None


class TocEntryParams(NamedTuple):
    item: tuple
    page_counts: dict
    tab_counts: count
    section_counts: count
    index_data: dict
    input_files: list


def _generate_toc_entry(toc_entry_params: TocEntryParams):
    """Generate a single TOC entry tuple for a given item from the index."""
    item = toc_entry_params.item
    page_counts = toc_entry_params.page_counts
    tab_counts = toc_entry_params.tab_counts
    section_counts = toc_entry_params.section_counts
    index_data = toc_entry_params.index_data
    input_files = toc_entry_params.input_files

    filename, (title, _, section) = item

    if section == "1":
        section_num = next(section_counts)
        return (f"SECTION_BREAK_{section_num}", title)

    # It's a file entry
    tab_number = f"{next(tab_counts):03}."
    current_page_start = page_counts["total"]

    # Find the full path for the file
    this_file_path = next((path for path in input_files if path.name == Path(filename).name), None)

    if not this_file_path or not this_file_path.exists():
        bundle_logger.warning(f"[MPCTE] File {filename} not found in input_files. Skipping.")
        return None

    try:
        with Pdf.open(this_file_path) as src:
            num_pages = len(src.pages)
            page_counts["total"] += num_pages

        entry_title, entry_date, _ = index_data.get(Path(filename).name, (Path(filename).stem, "Unknown", ""))
        if entry_date == "Unknown":
            entry_date = get_pdf_creation_date(this_file_path) or "Unknown"
    except Exception:
        bundle_logger.exception(f"Error processing file {this_file_path} for TOC.")
        return None
    else:
        return (tab_number, entry_title, entry_date, current_page_start)

def get_pages(input_files, filename) -> tuple[Pdf, list[Page]] | tuple[None, list]:
    this_file_path = next((path for path in input_files if path.name == Path(filename).name), None)
    if this_file_path and this_file_path.exists():
        try:
            src = Pdf.open(this_file_path)
            return src, src.pages[:]
        except Exception:
            bundle_logger.exception(f"Error merging file {this_file_path}")
    return None, []

def merge_pdfs_create_toc_entries(input_files, output_file, index_data: dict):
    """Merge PDFs and create table of contents entries.

    index_data is the roadmap for the bundle creation.
    1. Merge the PDFs in input_files into a single PDF at output_file.
    2. Create a table of contents from the index_data and return it.
    The table of contents is based on the index_data and the structural
    results of merging the files together.
     It outputs a list of tuples, toc_entries each containing:
        - tab number
        - title
        - date
        - page number
    """
    pdf = Pdf.new()
    page_counts = {"total": 0}  # Use a mutable dict to track page count across list comprehension
    tab_counts = count(1)
    section_counts = count(1)

    # Generate TOC entries
    toc_entries = [
        entry
        for item in index_data.items()
        if (entry := _generate_toc_entry(toc_entry_params=TocEntryParams(item, page_counts, tab_counts, section_counts, index_data, input_files)))
        is not None
    ]

    # Now, merge the PDFs in the correct order
    non_section_breaks = [filename for filename, (_, _, section) in index_data.items() if section != "1"]

    opened_pdfs: list[Pdf] = []
    try:
        for filename in non_section_breaks:
            src_pdf, pages = get_pages(input_files, filename)
            if src_pdf:
                opened_pdfs.append(src_pdf)
                pdf.pages.extend(pages)
            else:
                bundle_logger.warning(f"Could not get pages from {filename}. Skipping.")

        pdf.save(output_file)
        return toc_entries
    finally:
        # Ensure all source PDFs are closed after we are done with their pages
        for src in opened_pdfs:
            src.close()


def _create_bookmark_item(entry, length_of_frontmatter, bundle_config: BundleConfig):
    """Creates a single OutlineItem for a TOC entry based on bookmark settings."""
    tab_number, title, date, page = entry
    destination_page = page + length_of_frontmatter
    setting = bundle_config.bookmark_setting

    if setting == "tab-title":
        label = f"{tab_number} {title}"
    elif setting == "tab-title-date":
        label = f"{tab_number} {title} ({date})"
    elif setting == "tab-title-page":
        label = f"{tab_number} {title} [pg.{1 + destination_page}]"
    elif setting == "tab-title-date-page":
        label = f"{tab_number} {title} ({date}) [pg.{1 + destination_page}]"
    else:
        bundle_logger.error(f"[ABTP]Error: Unknown bookmark_setting: {setting}")
        # Fallback to the default bookmark style
        label = f"{tab_number} {title}"

    return OutlineItem(label, destination_page)


def add_bookmarks_to_pdf(pdf_file, output_file, toc_entries, length_of_frontmatter, bundle_config: BundleConfig):
    """Add outline entries ('bookmarks') to a PDF for navigation..

    It reads the digested toc_entries and adds an outline item for each.
    Due to loose naming conventions this can be confusing, so to be clear:
    - It does not bookmark the index itself (that's the job of bookmark_the_index).
    - It does not add on-page hyperlinks (that's add_hyperlinks)
    The content of the entry will depend on bookmark_setting from options:
        "tab-title" (default)
        "tab-title-date"
        "tab-title-page"
        "tab-title-date-page
    """
    with Pdf.open(pdf_file) as pdf:
        # Create all bookmark items at once using a list comprehension
        bookmark_items = [
            _create_bookmark_item(entry, length_of_frontmatter, bundle_config)
            for entry in toc_entries
            if "SECTION_BREAK" not in entry[0] and not ("tab" in str(entry[0]).lower() and "title" in str(entry[1]).lower())
        ]

        with pdf.open_outline() as outline:
            # Extend the root outline with the pre-built list of items
            outline.root.extend(bookmark_items)

        pdf.save(output_file)


def merge_frontmatter(input_files, output_file):
    """Merge uploaded coversheet and generated index.

    This is for cases where a coversheet is specified. The resulting
    frontmatter is pre-pended to the main bundle.
    """
    pdf = Pdf.new()
    for input_file in input_files:
        with Pdf.open(input_file) as src:
            pdf.pages.extend(src.pages)
        pdf.save(output_file)
    return output_file


def bookmark_the_index(pdf_file, output_file, coversheet=None):
    """The function add_bookmarks_to_pdf adds an outline item for each input file.

    But it cannot bookmark the index itself because it takes place earlier in the
    order of processing.
    This function comes back for a second pass and adds an outline item for the
    index.
    """
    with Pdf.open(pdf_file) as pdf:
        with pdf.open_outline() as outline:
            if coversheet:
                # test length of coversheet and set coversheet_length to the number of pages:
                with Pdf.open(coversheet) as coversheet_pdf:
                    coversheet_length = len(coversheet_pdf.pages)
                # Add an outline item for "Index" linking to the first page after the coversheet (it's 0-indexed):
                index_item = OutlineItem("Index", coversheet_length)
                outline.root.insert(0, index_item)
                bundle_logger.debug("[BTI]coversheet is specified, outline item added for index")
            else:
                # Add an outline item for "Index" linking to the first page:
                index_item = OutlineItem("Index", 0)
                outline.root.insert(0, index_item)
                bundle_logger.debug("[BTI]no coversheet specified, outline item added for index")
        pdf.save(output_file)


def _get_toc_pdf_styles(date_setting, index_font_setting):
    """Determine styles and column widths for the TOC PDF."""
    if index_font_setting == "serif":
        main_font, bold_font, base_font_size = "Times-Roman", "Times-Bold", 12
    elif index_font_setting == "sans":
        main_font, bold_font, base_font_size = "Helvetica", "Helvetica-Bold", 12
    elif index_font_setting == "mono":
        main_font, bold_font, base_font_size = "Courier", "Courier-Bold", 10
    elif index_font_setting == "traditional":
        main_font, bold_font, base_font_size = "Charter_regular", "Charter_bold", 12
    else:  # default to Helvetica
        main_font, bold_font, base_font_size = "Helvetica", "Helvetica-Bold", 12

    if date_setting == "hide_date":
        date_col_hdr, date_col_width, title_col_width, page_col_width = "", 0, 11.5, 2.5
    elif date_setting in ("YYYY-MM-DD", "DD-MM-YYYY", "MM-DD-YYYY", "uk_abbreviated_date", "us_abbreviated_date"):
        date_col_hdr, date_col_width, title_col_width, page_col_width = "Date", 3.2, 9.8, 1.7
    elif date_setting in ("uk_longdate", "us_longdate"):
        date_col_hdr, date_col_width, title_col_width, page_col_width = "Date", 4.2, 8.8, 1.7
    else:
        date_col_hdr, date_col_width, title_col_width, page_col_width = "Date", 3.5, 9.5, 1.7

    return {
        "main_font": main_font,
        "bold_font": bold_font,
        "base_font_size": base_font_size,
        "date_col_hdr": date_col_hdr,
        "date_col_width": date_col_width,
        "title_col_width": title_col_width,
        "page_col_width": page_col_width,
    }


def _create_header(row_tuple, style_sheet):
    """Creates a single formatted row for the ReportLab table."""
    row = list(row_tuple)
    # It's a regular data row
    return [Paragraph(str(cell), style_sheet["header_style"]) for cell in row]

class CreateReportlabRowParams(NamedTuple):
    row_tuple: tuple
    date_col_hdr: str
    dummy: bool | None
    page_offset: int
    style_sheet: StyleSheet1
    headers: tuple

def _create_reportlab_row(create_reportlab_row_params: CreateReportlabRowParams):
    """Creates a single formatted row for the ReportLab table."""
    (
        row_tuple,
        date_col_hdr,
        dummy,
        page_offset,
        style_sheet,
        headers
    ) = create_reportlab_row_params

    row = list(row_tuple)
    if all(x in row for x in headers):
        row[2] = date_col_hdr
        return [Paragraph(cell, style_sheet["main_style"]) for cell in row]
    if "SECTION_BREAK" in row[0]:
        row[0] = ""
        return [Paragraph(cell, style_sheet["bold_style"]) for cell in row]

    # It's a regular data row
    row[3] = 9999 if dummy else row[3] + page_offset
    return [Paragraph(str(cell), style_sheet["main_style"] if isinstance(cell, str) else style_sheet["main_style_right"]) for cell in row]

class TableDataParams(NamedTuple):
    toc_entries: list[tuple]
    date_col_hdr: str
    dummy: bool | None
    page_offset: int
    style_sheet: StyleSheet1
    bundle_title: str

def _build_reportlab_table_data(table_data_params: TableDataParams):
    """Build the data structure for the ReportLab table."""
    (
        toc_entries,
        date_col_hdr,
        dummy,
        page_offset,
        style_sheet,
        bundle_title
    ) = table_data_params
    list_of_section_breaks = [rowidx for rowidx, current_row_tuple in enumerate(toc_entries) if "SECTION_BREAK" in current_row_tuple[0]]

    headers = ("Tab", "Title", "Date", "Page")
    header_row = _create_header(headers, style_sheet)
    reportlab_table_data = [
        _create_reportlab_row(CreateReportlabRowParams(row, date_col_hdr, dummy, page_offset, style_sheet, headers))
        for row in toc_entries
    ]

    reportlab_table_data.insert(0, header_row)  # Insert header row at the top

    # Adjust section break indices to account for the inserted header row.
    adjusted_section_breaks = [idx + 1 for idx in list_of_section_breaks]
    return reportlab_table_data, adjusted_section_breaks


def _setup_reportlab_styles(main_font, bold_font, base_font_size):
    """Set up ParagraphStyle objects for ReportLab."""
    script_dir = Path(__file__).parent
    static_dir = script_dir / "static"

    # Register non-standard fonts.
    pdfmetrics.registerFont(TTFont("Charter_regular", static_dir / "Charter_Regular.ttf"))
    pdfmetrics.registerFont(TTFont("Charter_bold", static_dir / "Charter_Bold.ttf"))
    pdfmetrics.registerFont(TTFont("Charter_italic", static_dir / "Charter_Italic.ttf"))
    reportlab.rl_config.warnOnMissingFontGlyphs = 0

    # Set up stylesheet for the various styles used..
    styleSheet = getSampleStyleSheet()


    header_style = ParagraphStyle(
        "header_style",
        parent=styleSheet["Normal"],
        fontName=bold_font,
        fontSize=base_font_size,
        leading=14,
        alignment=TA_CENTER,
    )
    main_style = ParagraphStyle(
        "main_style",
        parent=styleSheet["Normal"],
        fontName=main_font,
        fontSize=base_font_size,
        leading=14,
    )
    main_style_right = ParagraphStyle(
        "main_style_right",
        parent=styleSheet["Normal"],
        fontName=main_font,
        fontSize=base_font_size,
        leading=14,
        alignment=TA_RIGHT,
    )

    bold_style = ParagraphStyle(
        "bold_style",
        parent=styleSheet["Normal"],
        fontName=bold_font,
        fontSize=base_font_size,
        leading=14,
    )
    claimno_style = ParagraphStyle(
        "claimno_style",
        parent=styleSheet["Normal"],
        fontName=bold_font,
        fontSize=base_font_size,
        leading=14,
        alignment=TA_RIGHT,
    )
    bundle_title_style = ParagraphStyle(
        "bundle_title_style",
        parent=styleSheet["Normal"],
        fontName=bold_font,
        fontSize=base_font_size + 6,
        leading=14,
        alignment=TA_CENTER,
    )
    case_name_style = ParagraphStyle(
        "case_name_style",
        parent=styleSheet["Normal"],
        fontName=bold_font,
        fontSize=base_font_size + 2,
        leading=14,
        alignment=TA_CENTER,
    )

    styles = [
        header_style,
        main_style,
        main_style_right,
        bold_style,
        claimno_style,
        bundle_title_style,
        case_name_style,
        # footer_style,  # Footer style is not used in the TOC PDF
    ]

    for style in styles:
        styleSheet.add(ParagraphStyle(name=style.name, parent=style))

    return styleSheet


def create_toc_pdf_reportlab(toc_entries, casedetails: dict[str, str], bundle_config: BundleConfig, output_file, options: dict):
    """Generate a table of contents PDF using ReportLab."""
    styles = _get_toc_pdf_styles(options.get("date_setting"), bundle_config.index_font)
    main_font = styles["main_font"]
    bold_font = styles["bold_font"]
    base_font_size = styles["base_font_size"]
    date_col_hdr = styles["date_col_hdr"]
    date_col_width = styles["date_col_width"]
    title_col_width = styles["title_col_width"]
    page_col_width = styles["page_col_width"]

    page_offset = 0 if options.get("dummy") else bundle_config.expected_length_of_frontmatter + 1
    styleSheet = _setup_reportlab_styles(main_font, bold_font, base_font_size)

    # Now, position each element within a table.
    # There are three tables: Claim no, [Case title, bundle title], and [toc_entries]
    # Each table is defined by:
    #  - define data to go into the table;
    #  - define the table itself; and #  - set the style of the table.
    # Finally, they are passed as elements to the builder function.
    reportlab_pdf = SimpleDocTemplate(
        str(output_file), pagesize=A4, rightMargin=1.5 * cm, leftMargin=1.5 * cm, topMargin=1 * cm, bottomMargin=1.5 * cm
    )

    reportlab_pdf = SimpleDocTemplate(
        str(output_file), pagesize=A4, rightMargin=1.5 * cm, leftMargin=1.5 * cm, topMargin=1 * cm, bottomMargin=1.5 * cm
    )

    # Claim No table - top right
    claimno_table_data = [
        [Paragraph(casedetails.get("claim_no", ""), styleSheet["claimno_style"])],  # Claim No
    ]
    claimno_table = Table(
        data=claimno_table_data,
        colWidths=PAGE_WIDTH * 0.9,
        rowHeights=1.5 * cm,
    )
    claimno_table.setStyle(
        TableStyle(
            [
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 50),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
                # ('GRID', (0, 0), (-1, -1), 0.5, 'black'),
            ]
        )
    )


    bundle_title = casedetails.get("bundle_title", "")
    # Now, the case name and bundle title:
    if not options.get("confidential"):
        header_table_data = [
            ["", Paragraph(casedetails.get("case_name", ""), styleSheet["case_name_style"]), ""],  # Case Name
            ["", Paragraph(bundle_title, styleSheet["bundle_title_style"]), ""],  # Bundle Title
        ]
    else:
        header_table_data = [
            ["", Paragraph(casedetails.get("case_name", ""), styleSheet["case_name_style"]), ""],  # Case Name
            ["", Paragraph((f'<font color="red">CONFIDENTIAL</font> {casedetails.get("bundle_title", "")}'), styleSheet["bundle_title_style"]), ""],
            # Bundle Title
        ]
    header_table = Table(header_table_data, colWidths=[PAGE_WIDTH / 8, PAGE_WIDTH * (6 / 8), PAGE_WIDTH / 8])  # aesthetic choice
    header_table.setStyle(
        TableStyle(
            [
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("ALIGN", (2, 0), (2, 0), "RIGHT"),  # Align Claim No to the right
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("SIZE", (0, 0), (-1, -1), 10),
                ("LINEBELOW", (1, 1), (1, 1), 1, colors.black),  # Underline Bundle Title
                ("LINEABOVE", (1, 1), (1, 1), 1, colors.black),  # Overline Bundle Title
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
            ]
        )
    )

    style_sheet = styleSheet

    # Third, the main toc entries able:
    reportlab_table_data, list_of_section_breaks = _build_reportlab_table_data(
        TableDataParams(toc_entries, date_col_hdr, options.get("dummy"), page_offset, style_sheet, bundle_title)
    )
    toc_table = Table(
        reportlab_table_data,
        colWidths=[1.3 * cm, title_col_width * cm, date_col_width * cm, page_col_width * cm],
        repeatRows=1,
        cornerRadii=(5, 5, 0, 0),
    )
    style = TableStyle(
        [
            # Style for header row:
            ("BACKGROUND", (0, 0), (-1, 0), colors.darkgray),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("LINEBELOW", (0, 0), (-1, 0), 1, colors.black),
            # ('FONTNAME', (0, 0), (-1, 0Roman-), bold_fontname),
            ("ALIGNMENT", (0, 0), (-1, 0), "CENTRE"),
            ("FONTSIZE", (0, 0), (-1, 0), 12),
            # rest of table:
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("BACKGROUND", (0, 1), (-1, -1), colors.white),
            ("LINEBELOW", (0, 1), (-1, -1), 0.3, colors.black),
            # paint section breaks with grey background:
        ]
    )
    for section_break_row in list_of_section_breaks:
        style.add("BACKGROUND", (0, int(section_break_row)), (-1, int(section_break_row)), colors.lightgrey)

    toc_table.setStyle(style)

    # Now, add a footer with the page number. Use a single-cell table at the bottom of the page:
    # current page number:

    # footer_frame = Frame (
    #     PAGE_WIDTH*0.2, 1*cm, #x, y lower left
    #     PAGE_WIDTH*0.8, 1.5*cm, #box width and height
    #     leftPadding=6,
    #     bottomPadding=6,
    #     rightPadding=6,
    #     topPadding=6,
    #     id="footerframe",
    #     showBoundary=1
    # )
    # footer_frame.add("Blob", reportlab_pdf)

    # Now, build the pdf:
    elements = [claimno_table, header_table, Spacer(1, 1 * cm), toc_table]
    if not options.get("roman_numbering"):
        footer_config_with_bundle = functools.partial(reportlab_footer_config, bundle_config=bundle_config)
        reportlab_pdf.build(elements, onFirstPage=footer_config_with_bundle, onLaterPages=footer_config_with_bundle)
    else:
        reportlab_pdf.build(elements)


def generate_footer_pages_reportlab(filename, num_pages, bundle_config):
    """Generate a PDF with N blank pages, using onFirstPage and onLaterPages callbacks.

    Args:
        filename (str): The name of the output PDF file.
        num_pages (int): Number of blank pages to create.
        onFirstPage (callable): Callback for the first page.
        onLaterPages (callable): Callback for subsequent pages.
        bundle_config: configuration object.

    """
    bundle_logger.debug(f"[GFP]Generating {num_pages} blank pages in {filename}")
    # Create the document
    # pylint: disable=E1123
    doc = SimpleDocTemplate(
        str(filename),
        pagesize=A4,
    )
    # ReportLab protects against infinite loops by checking whether or not a
    # page has content at build time, and terminates after 10 pages without
    # content. It doesn't count footer content as content. So, it breaks when
    # generating footer-only pages.
    # Workaround: Since reportlab defines 'content' in this sense  as anything
    # which is a flowable, a workaround is to add an invisible flowable to each page.
    annoying_blank_flowable = Paragraph("")

    # Prepare blank pages with PageBreaks
    story: list[Flowable] = [item for _ in range(num_pages) for item in (annoying_blank_flowable, PageBreak())]

    # Build the document with the footer config:
    footer_config_with_bundle = functools.partial(reportlab_footer_config, bundle_config=bundle_config)
    doc.build(story, onFirstPage=footer_config_with_bundle, onLaterPages=footer_config_with_bundle)


def reportlab_footer_config(canvas, doc, bundle_config: BundleConfig):
    """Configure the footer for ReportLab documents.

    the other reportlab functions during their build process.
    It's not used directly, and since it's internal to ReportLab,
    it's easier to operate on global variables here.
    """
    length_of_frontmatter_offset = bundle_config.expected_length_of_frontmatter if bundle_config.expected_length_of_frontmatter else 0
    total_number_of_pages = bundle_config.total_number_of_pages if bundle_config.total_number_of_pages else 0
    page_num_alignment = bundle_config.page_num_align if bundle_config.page_num_align else None
    page_num_font = bundle_config.footer_font if bundle_config.footer_font else None
    page_numbering_style = bundle_config.page_num_style if bundle_config.page_num_style else None
    footer_prefix = bundle_config.footer_prefix if bundle_config.footer_prefix else ""

    def set_footer_font_and_base(page_num_font):
        if page_num_font == "serif":
            footer_font = "Times-Roman"
            footer_base_font_size = 15
        elif page_num_font == "Helvetica":
            footer_font = "sans"
            footer_base_font_size = 14
        elif page_num_font == "mono":
            footer_font = "Courier"
            footer_base_font_size = 14
        elif page_num_font == "traditional":
            footer_font = "Charter_regular"
            footer_base_font_size = 15
        else:  # defalt to Helvetica
            footer_font = "Helvetica"
            footer_base_font_size = 14
        return footer_font, footer_base_font_size

    footer_font, footer_base_font_size = set_footer_font_and_base(page_num_font)

    canvas.saveState()
    canvas.setFont("Times-Bold", 16)
    if page_num_alignment == "left":
        footer_style = ParagraphStyle(
            "BodyText",
            fontSize=footer_base_font_size,
            fontName=footer_font,
            # leading=14
            alignment=TA_LEFT,
        )
    elif page_num_alignment == "right":
        footer_style = ParagraphStyle(
            "BodyText",
            fontSize=footer_base_font_size,
            fontName=footer_font,
            # leading=14
            alignment=TA_RIGHT,
        )
    elif page_num_alignment == "centre":
        footer_style = ParagraphStyle(
            "BodyText",
            fontSize=footer_base_font_size,
            fontName=footer_font,
            # leading=14
            alignment=TA_CENTER,
        )
    else:
        footer_style = ParagraphStyle(
            "BodyText",
            fontSize=footer_base_font_size,
            fontName=footer_font,
            # leading=14
            alignment=TA_RIGHT,
        )

    def _get_page_number_string(style, page_num, offset, total_pages):
        """Get formatted page number string based on style."""
        current_page = page_num + offset

        style_formats = {
            "x": f"{current_page}",
            "x_of_y": f"{page_num} of {total_pages}",
            "page_x": f"Page {current_page}",
            "page_x_of_y": f"Page {current_page} of {total_pages}",
            "x_slash_y": f"{current_page} / {total_pages}",
        }
        return style_formats.get(style, f"Page {current_page}")

    # Get the page number string
    page_number_str = _get_page_number_string(page_numbering_style, canvas.getPageNumber(), length_of_frontmatter_offset, total_number_of_pages)

    # Build the complete footer data string in one assignment
    footer_data = f"{footer_prefix.strip()} {page_number_str}" if footer_prefix else page_number_str

    footer_frame = Frame(
        0,
        0 * cm,  # x, y lower left
        PAGE_WIDTH,
        1.5 * cm,  # box width and height
        leftPadding=50,
        bottomPadding=0,
        rightPadding=50,
        topPadding=0,
        id="footerframe",
        showBoundary=0,
    )
    # footer_frame.hAlign = 'RIGHT' # hAlign is not a valid attribute for Frame
    footer_frame.add(Paragraph(footer_data, footer_style), canvas)


def get_index_font_family(index_font):
    if index_font == "sans":
        index_font_family = "phv"  # LaTeX font family for Helvetica, see https://www.overleaf.com/learn/latex/Font_typefaces#Reference_guide
        bundle_logger.debug("[CTP]..Sans-serif font selected for TOC")
    elif index_font == "serif":
        index_font_family = "ppl"  # LaTeX font family for Palatino
        bundle_logger.debug("[CTP]..Serif font selected for TOC")
    elif index_font == "mono":
        index_font_family = "pcr"  # LaTeX font family for Courier
        bundle_logger.debug("[CTP]..Monospace font selected for TOC")
    else:
        index_font_family = ""  # Default to Computer modern
        bundle_logger.debug("[CTP]..No font setting provided, using default font for TOC")
    return index_font_family


def get_footer_alignment_setting(page_num_align):
    # parse alignment setting
    if page_num_align == "left":
        footer_alignment_setting = r"LO LE"
        bundle_logger.debug("[MPNP]..Left alignment selected for page numbers")
    elif page_num_align == "right":
        footer_alignment_setting = r"RO RE"
        bundle_logger.debug("[MPNP]..Right alignment selected for page numbers")
    elif page_num_align == "centre":
        footer_alignment_setting = r"CO CE"
        bundle_logger.debug("[MPNP]..Centre alignment selected for page numbers")
    else:
        footer_alignment_setting = r"CO CE"
        bundle_logger.debug("[MPNP]..Defaulting to centre alignment for page numbers")
    return footer_alignment_setting


def get_footer_font(_footer_font):
    # parse font setting
    if _footer_font == "sans":
        footer_font = "phv"  # LaTeX font family for Helvetica, see https://www.overleaf.com/learn/latex/Font_typefaces#Reference_guide
        bundle_logger.debug("[MPNP]..Sans-serif font selected for page numbers")
    elif _footer_font == "serif":
        footer_font = "ppl"  # LaTeX font family for Palatino
        bundle_logger.debug("[MPNP]..Serif font selected for page numbers")
    elif _footer_font == "mono":
        footer_font = "pcr"  # LaTeX font family for Courier
        bundle_logger.debug("[MPNP]..Monospace font selected for page numbers")
    else:
        footer_font = "cmr"  # LaTeX font family for Courier by default
        bundle_logger.debug("[MPNP]..defaulting to Computer Modern Roman text font for page numbers")
    return footer_font


def get_footer_text(footer_prefix, page_num_style, main_page_count, frontmatter_offset):
    # Map page numbering styles to their format strings and log messages
    style_data = {
        "x": (r"\thepage", "x"),
        "x_of_y": (r"\thepage{} of " + str(main_page_count + frontmatter_offset), "x of y"),
        "page_x": (r"Page \thepage", "Page x"),
        "page_x_of_y": (r"Page \thepage{} of " + str(main_page_count + frontmatter_offset), "Page x of y"),
        "x_slash_y": (r"\thepage /" + str(main_page_count + frontmatter_offset), "x / y"),
    }

    # Get style data or use defaults
    page_part, style_name = style_data.get(page_num_style, (r"Page \thepage", "Page x"))
    bundle_logger.debug(f"[MPNP]..Page numbering style: {style_name}")

    # Build the complete string in one assignment
    if footer_prefix:
        sanitized_prefix = sanitise_latex(footer_prefix.strip() + " ")
        bundle_logger.debug(f"[MPNP]..Prefixing page numbers with '{sanitized_prefix}'")
        return sanitized_prefix + page_part
    else:
        return page_part


toc_content_prefix = r"""
\documentclass[12pt,a4paper]{article}
\usepackage{fancyhdr}
\usepackage{geometry}
\usepackage{hyperref}
\usepackage{longtable}
\usepackage{color, colortbl}
"""


def get_non_roman(footer_font, starting_page, footer_alignment_setting, footer_text):
    return f"""
        \\newcommand{{\\fontsetting}}{{\\fontfamily{{{footer_font}}}\\fontseries{{b}}\\base_font_size{{18}}{{22}}\\selectfont}}
        \\setcounter{{page}}{{{starting_page}}}
        \\begin{{document}}
        \\pagestyle{{fancy}}
        \\renewcommand{{\\headrulewidth}}{{0pt}}
        \\setlength{{\\footskip}}{{20pt}}
        \\fancyhf{{}} % to clear the header and the footer simultaneously
        \\fancyfoot[{footer_alignment_setting}]{{\\fontsetting {footer_text}}}
        """


def get_last_foot(date_col_width, date_col_hdr):
    return rf"""
        \def\arraystretch{{1.3}}
        \begin{{longtable}}{{p{{1.2cm}} p{{10cm}} p{{{date_col_width}}} r}}
        \hline
        \textbf{{Tab}} & \textbf{{Title}} & \textbf{{{date_col_hdr}}} & \textbf{{Page}} \\
        \hline
        \endfirsthead
        \hline
        \textbf{{Tab}} & \textbf{{Title}} & \textbf{{{date_col_hdr}}} & \textbf{{Page}} \\
        \hline
        \endhead
        \hline
        \endfoot
        \hline
        \endlastfoot
        """


bn1 = r"""
\begin{center}
\rule{0.5\linewidth}{0.3mm} \\
\vspace{0.3cm}
"""
bn2 = r"""
\rule{0.5\linewidth}{0.3mm} \\
\vspace{-0.5cm}
\end{center}
"""


class FooterTexConfig(NamedTuple):
    """Configuration for TeX-based footer generation."""

    length_of_frontmatter_offset: int
    main_page_count: int
    page_num_alignment: str
    page_num_font: str
    page_numbering_style: str
    footer_prefix: str


def _get_tex_font_settings(bundle_config_index_font):
    """Determine LaTeX font family for TOC."""
    if bundle_config_index_font == "sans":
        return "phv"  # Helvetica
    if bundle_config_index_font == "serif":
        return "ppl"  # Palatino
    if bundle_config_index_font == "mono":
        return "pcr"  # Courier
    return ""  # Default to Computer modern


def _get_tex_alignment_setting(bundle_config_page_num_align):
    """Determine LaTeX alignment setting for footer."""
    if bundle_config_page_num_align == "left":
        return r"LO LE"
    if bundle_config_page_num_align == "right":
        return r"RO RE"
    if bundle_config_page_num_align == "centre":
        return r"CO CE"
    return r"CO CE"  # Default


def _get_tex_page_numbering_text(config: FooterTexConfig, footer_text_prefix):
    """Determine LaTeX page numbering style text for footer."""
    page_numbering_style = config.page_numbering_style
    if page_numbering_style == "x":
        return footer_text_prefix + r"\thepage"
    if page_numbering_style == "x_of_y":
        return footer_text_prefix + r"\thepage{} of " + str(config.main_page_count + config.length_of_frontmatter_offset)
    if page_numbering_style == "page_x":
        return footer_text_prefix + r"Page \thepage"
    if page_numbering_style == "page_x_of_y":
        return footer_text_prefix + r"Page \thepage{} of " + str(config.main_page_count + config.length_of_frontmatter_offset)
    if page_numbering_style == "x_slash_y":
        return footer_text_prefix + r"\thepage /" + str(config.main_page_count + config.length_of_frontmatter_offset)
    return footer_text_prefix + r"Page \thepage"  # Default


def generate_footer_pages_tex(
    page_numbers_tex_path,
    config: FooterTexConfig,
):
    """Generate a PDF of page numbers using TeX.

    replaced by Reportlab version: make_page_numbers_pdf_reportlab.
    """
    starting_page = config.length_of_frontmatter_offset + 1
    footer_alignment_setting = _get_tex_alignment_setting(config.page_num_alignment)
    footer_font = _get_tex_font_settings(config.page_num_font)
    footer_text_prefix = sanitise_latex(config.footer_prefix.strip() + " ") if config.footer_prefix else ""
    footer_text = _get_tex_page_numbering_text(config, footer_text_prefix)

    # Create LaTeX file for page numbers
    page_number_footer_tex = rf"""
        \documentclass[12pt,a4paper]{{article}}
        \usepackage{{fancyhdr}}
        \usepackage{{multido}}
        \usepackage[hmargin=.8cm,vmargin=1.1cm,nohead,nofoot,twoside]{{geometry}}
        \newcommand{{\fontsetting}}{{\fontfamily{{{footer_font}}}\fontseries{{b}}\fontsize{{18}}{{22}}\selectfont}}
        \setcounter{{page}}{{{starting_page}}}
        \begin{{document}}
        \pagestyle{{fancy}}
        \renewcommand{{\headrulewidth}}{{0pt}}
        \setlength{{\footskip}}{{20pt}}
        \fancyhf{{}} % to clear the header and the footer simultaneously
        \fancyfoot[{footer_alignment_setting}]{{\fontsetting {footer_text}}}
        \multido{{}}{{{config.main_page_count}}}{{\vphantom{{x}}\newpage}}
        \end{{document}}
        """

    with Path(page_numbers_tex_path).open("w") as f:
        f.write(page_number_footer_tex)
    bundle_logger.debug(f"[MPNP]Page numbers content written to file: {page_numbers_tex_path}")

    page_numbers_pdf_path = str(page_numbers_tex_path).replace(".tex", ".pdf")
    # Compile LaTeX file to PDF
    result = os.system(f"pdflatex -output-directory {Path(page_numbers_pdf_path).parent} {page_numbers_tex_path} > /dev/null")
    if result != 0:
        bundle_logger.error(f"[MPNP]pdflatex command failed with error code {result}")
    else:
        bundle_logger.debug(f"[MPNP]pdflatex command succeeded. Page numbers PDF saved to {page_numbers_pdf_path}")
    return page_numbers_pdf_path


def add_footer_to_bundle(input_file, page_numbers_pdf_path, output_file):
    """Overlay a footer PDF onto a content PDF.

    Given an input file (a series of pdfs merged together) and
    a pdf of equal length containing only the page number footers,
    this combines the two by overlaying footers on top of the input file.
    It scales the footer according to horizontal scaling factor (an imperfect
    solution to a difficult problem)
    """
    # CONVERSION NOTE: PDF points are 1/72 inch by standard..
    # the scaling factor between point and mm is 2.8346...
    # a4 paper (which I've chosen for the reference page numbering) is 210mm x 297mm = 595 x 842 points.
    # This is the reference page numbering for A4 paper size.
    # Load the input PDF and the page numbers PDF
    input_pdf = PdfReader(input_file)
    page_numbers_pdf = PdfReader(page_numbers_pdf_path)

    # Ensure the number of pages match
    if len(input_pdf.pages) != len(page_numbers_pdf.pages):
        msg = f"Page counts do not match: input={len(input_pdf.pages)} vs page numbers={len(page_numbers_pdf.pages)}"
        bundle_logger.error("[OPN]Error overlaying page numbers")
        raise ValueError(msg)

    try:
        # Create a writer for the output PDF
        writer = PdfWriter()

        # Overlay page numbers PDF pages onto input PDF pages
        for input_page, overlay_page in zip(input_pdf.pages, page_numbers_pdf.pages, strict=True):
            # The content is `input_page`, the footer is `overlay_page`.
            # We merge the footer ONTO the content page, scaling it to match the width.
            scaling_factor = float(input_page.mediabox.width / overlay_page.mediabox.width)
            input_page.merge_scaled_page(overlay_page, scaling_factor)
            writer.add_page(input_page)

        # Write the resulting PDF to the output file
        with Path(output_file).open("wb") as f:
            writer.write(f)
    except Exception:
        bundle_logger.exception("[OPN]Error overlaying page numbers")
        raise


def pdf_paginator_reportlab(input_file, bundle_config: BundleConfig, output_file):
    """Drop-in replacement for tex alternative.

    Calls sub-functions to create page numbers and add them to the bundle.
    """
    bundle_logger.debug("[PPRL]Paginate PDF function beginning (ReporLab version)")
    main_page_count = 0
    try:
        main_page_count = len(Pdf.open(input_file).pages)
        bundle_logger.debug(f"[PPRL]..Main PDF has {main_page_count} pages")
    except Exception:
        bundle_logger.exception("[PPRL]..Error counting pages in TOC")
        raise
    page_numbers_pdf_path = Path(output_file).parent / "pageNumbers.pdf"
    generate_footer_pages_reportlab(page_numbers_pdf_path, main_page_count, bundle_config)
    if Path(page_numbers_pdf_path).exists():
        try:
            add_footer_to_bundle(input_file, page_numbers_pdf_path, output_file)
            bundle_logger.debug("[PPRL]Page numbers overlaid on main PDF")
        except Exception:
            bundle_logger.exception("[PPRL]Error overlaying page numbers")
            raise
    else:
        bundle_logger.error("[PPRL]Error creating page numbers PDF!")
    return main_page_count


def pdf_paginator_tex(input_file, output_file, bundle_config: BundleConfig, frontmatter_offset):
    """Manage pagination using TeX.

    This is the pagination manager for generate_footer_pages_tex and add_footer_to_bundle.
    It makes sure they are supplied with the correct information.
    """
    bundle_logger.debug("[PPPaginate PDF function beginning")
    main_page_count = 0
    try:
        main_page_count = len(Pdf.open(input_file).pages)
        bundle_logger.debug(f"[PP..Main PDF opened with {main_page_count} pages")
    except Exception:
        bundle_logger.exception("[PP..Error counting pages in TOC")
        raise
    output_dir = Path(output_file).parent
    page_numbers_tex_path = output_dir / "pageNumbers.tex"
    footer_config = FooterTexConfig(
        main_page_count,
        frontmatter_offset,
        bundle_config.page_num_align,
        bundle_config.footer_font,
        bundle_config.page_num_style,
        bundle_config.footer_prefix,
    )
    page_numbers_pdf_output = generate_footer_pages_tex(page_numbers_tex_path, footer_config)

    if Path(page_numbers_pdf_output).exists():
        try:
            add_footer_to_bundle(input_file, page_numbers_pdf_output, output_file)
            bundle_logger.debug("[PP] Page numbers overlaid on main PDF")
        except Exception:
            bundle_logger.exception("[PPError overlaying page numbers")
            raise
    else:
        bundle_logger.error("[PPError creating page numbers PDF: see pdftex temporary logs in temp folder.")
    return main_page_count


def add_roman_labels(pdf_file, length_of_frontmatter, output_file):
    """Adjust page numbering to begin with Roman numerals for the frontmatter.

    This begins with page 1 on the first page of the main
    content.
    The elegant solution which is so often messed up that nobody wants to
    go near it any more.
    """
    bundle_logger.debug(f"[APL]Adding page labels to PDF {pdf_file}")
    with Pdf.open(pdf_file) as pdf:
        nums = [
            0,
            Dictionary(S=Name("/r")),  # lowercase Roman starting at first page of bundle
            length_of_frontmatter,
            Dictionary(S=Name("/D")),  # Decimal starting at page 1 after frontmatter
        ]

        pdf.Root.PageLabels = Dictionary(Nums=nums)
        pdf.save(output_file)


def process_csv_index(csv_index):
    """Process CSV index data from a string..

    This is a stub of a test idea to allow passing csv info as a raw argument
    via command line.
    The functionality has been overtaken by the frontend-generated
    CSV file.
    """
    index_data = {}

    for row in csv_index:
        if (row["Type"] == "File") and (row["Filename"] not in index_data):
            index_data[row["Filename"]] = (row["Title"], row["Date"], row["Section"])

    return index_data


def transform_coordinates(coords, page_height):
    """Transform coordinates from top-left to bottom-left origin system."""
    x1, y1, x2, y2 = coords
    # Flip the y coordinates by subtracting from page height
    new_y1 = page_height - y2  # Note: we swap y1 and y2 here
    new_y2 = page_height - y1
    return (x1, new_y1, x2, new_y2)


def add_annotations_with_transform(pdf_file, list_of_annotation_coords, output_file):
    """Write hyperlinks into the output bundle PDF.

    hyperlinks into the output bundle PDF.
    It's only called as a subprocess of add_hyperlinks.
    """
    reader = PdfReader(pdf_file)
    writer = PdfWriter()

    # Copy all pages to the writer
    for page in reader.pages:
        writer.add_page(page)

    # navigate the treacherous PDF coordinate system
    for annotation in list_of_annotation_coords:
        toc_page = annotation["toc_page"]
        coords = annotation["coords"]
        destination_page = annotation["destination_page"]

        # Get the page height for coordinate transformation
        page = reader.pages[toc_page]
        page_height = float(page.mediabox.height)

        # Transform the coordinates
        transformed_coords = transform_coordinates(coords, page_height)

        try:
            # Create link annotation with transformed coordinates
            link = Link(rect=transformed_coords, target_page_index=destination_page, fit=Fit("/FitH"))
            writer.add_annotation(page_number=toc_page, annotation=link)

            # # Create highlight annotation with transformed coordinates
            # quad_points = [
            #     transformed_coords[0], transformed_coords[3],  # x1, y1 (top left)
            #     transformed_coords[2], transformed_coords[3],  # x2, y1 (top right)
            #     transformed_coords[0], transformed_coords[1],  # x1, y2 (bottom left)
            #     transformed_coords[2], transformed_coords[1]   # x2, y2 (bottom right)
            # ]
            bundle_logger.debug(f"[AAWT]Added annotations on TOC page {toc_page} to destination pg index {destination_page}")

        except Exception:
            bundle_logger.exception(f"[AAWT]Failed to add annotations on TOC page {toc_page}")
            raise

    # Write the output file
    with Path(output_file).open("wb") as output:
        writer.write(output)


class FindHyperlinkMatchForEntryParams(NamedTuple):
    entry: tuple[str, str, str, str]
    scraped_pages_text: list[list[dict[str, float | str]]]
    length_of_coversheet: int
    length_of_frontmatter: int
    date_setting: str
    roman_page_labels: bool


def _find_hyperlink_match_for_entry(
    find_hyperlink_match_for_entry_params: FindHyperlinkMatchForEntryParams,
) -> dict[str, int | str | tuple[float, float, float, float]] | None:
    """Find the coordinates and page for a single TOC entry to create a hyperlink."""
    (
        entry,
        scraped_pages_text,
        length_of_coversheet,
        length_of_frontmatter,
        date_setting,
        roman_page_labels,
    ) = find_hyperlink_match_for_entry_params

    def _create_search_patterns(entry_data):
        """Create regex patterns to find the TOC entry in the extracted text."""
        tab_key = re.escape(entry_data[0].replace(" ", ""))
        is_long_title = len(entry_data[1]) > MAX_TITLE_LENGTH_FOR_HYPERLINK_SEARCH
        title_key = re.escape(
            entry_data[1][: MAX_TITLE_LENGTH_FOR_HYPERLINK_SEARCH - 1].replace(" ", "") if is_long_title else entry_data[1].replace(" ", "")
        )
        page_key = str(int(entry_data[3]) + 1 if roman_page_labels else int(entry_data[3]) + length_of_frontmatter + 1)

        fallback_pattern = re.compile(f"{tab_key}.*?{page_key}")

        if date_setting == "hide_date":
            main_pattern_str = f"{tab_key}{title_key}.*?{page_key}" if is_long_title else f"{tab_key}{title_key}{page_key}"
            main_pattern = re.compile(main_pattern_str)
        else:
            date_key = re.escape(entry_data[2].replace(" ", ""))
            main_pattern_str = f"{tab_key}{title_key}.*?{date_key}{page_key}" if is_long_title else f"{tab_key}{title_key}{date_key}{page_key}"
            main_pattern = re.compile(main_pattern_str)

        return [main_pattern, fallback_pattern]

    bundle_logger.debug(f"[HYP]..Processing TOC entry: {entry}")
    patterns = _create_search_patterns(entry)

    # Search through the scraped text for a match
    for page_idx, page_lines in enumerate(scraped_pages_text, start=length_of_coversheet):
        for line in page_lines:
            stripped_line_text = str(line["text"]).replace(" ", "")
            for pattern in patterns:
                if pattern.search(stripped_line_text):
                    bundle_logger.debug(f"[HYP]....found on page {page_idx} with pattern '{pattern.pattern}'")
                    return {
                        "title": entry[1],
                        "toc_page": int(page_idx),
                        "coords": (
                            float(line["x0"]),
                            float(line["bottom"]),
                            float(line["x1"]),
                            float(line["top"]),
                        ),
                        "destination_page": int(entry[3]) + length_of_frontmatter,
                    }
    return None


class AddHyperlinksParams(NamedTuple):
    pdf_file: Path
    output_file: Path
    length_of_coversheet: int
    length_of_frontmatter: int
    toc_entries: list[tuple[str, str, str, str]]
    date_setting: str = "show_date"
    roman_page_labels: bool = False

def get_scraped_pages_text(pdf: PDF, idx: int):
    current_page = pdf.pages[idx]
    bundle_logger.debug(f"[HYP]..Processing page {idx} for TOC text extraction")
    return current_page.extract_text_lines()

def _find_match_for_entry(entry, scraped_pages_text, length_of_coversheet, length_of_frontmatter):
    """Find the first matching line in the scraped text for a given TOC entry."""
    tab_to_find = str(entry[0])
    bundle_logger.debug(f"[HYP]....Searching for tab: '{tab_to_find}'")
    for page_idx, page_lines in enumerate(scraped_pages_text, start=length_of_coversheet):
        if not page_lines:
            continue
        for line in page_lines:
            line_text = str(line.get("text", ""))
            if line_text.strip().startswith(tab_to_find):
                bundle_logger.debug(f"[HYP]......SUCCESS: Found tab '{tab_to_find}' on page {page_idx} in line: '{line_text}'")
                return {
                    'title': entry[1],
                    'toc_page': page_idx,
                    'coords': (line['x0'], line['bottom'], line['x1'], line['top']),
                    'destination_page': int(entry[3]) + length_of_frontmatter
                }
    bundle_logger.warning(f"[HYP]......FAILURE: No match found for tab '{tab_to_find}'")
    return None

def add_hyperlinks(
        pdf_file,
        output_file,
        length_of_coversheet,
        length_of_frontmatter,
        toc_entries
):
    """Add Hyperlinks to the table of contents pages.

    The PDF standard defines these as
    rectangular areas with an action to jump to a destination within the document.

    This means we need to know the coordinates of the rectangles. That is the main
    job of this function: to find rectangle coordinates.

    Strategy:
    - extract the text of the toc pages into a list of words with coordinates.
    - create a search string for each intended hyperlink entry (a melange of the expected tab, title, page).
    - truncate the string to account for line breaks and noise.
    - find that search string in the extracted text; thus, find the coordinates on the page.
    - pass off to the annotation writer for actual writing.
    """
    bundle_logger.debug("[HYP]Starting hyperlink addition")

    # Step 1: Extract text and coordinates from TOC
    with pdfplumber.open(pdf_file) as pdf:
        scraped_pages_text = [get_scraped_pages_text(pdf, idx) for idx in range(length_of_coversheet, length_of_frontmatter)]

    list_of_annotation_coords = [
        match
        for entry in toc_entries
        if "SECTION_BREAK" not in entry[0] and not (len(entry) > MIN_TOC_ENTRY_FIELDS and str(entry[3]) == "Page")
        if (match := _find_match_for_entry(entry, scraped_pages_text, length_of_coversheet, length_of_frontmatter))
    ]

    # Step 3: Add annotations to the PDF
    add_annotations_with_transform(pdf_file, list_of_annotation_coords, output_file)


class TocTexConfig(NamedTuple):
    bundle_config: BundleConfig
    roman_numbering: bool
    length_of_coversheet: int
    frontmatter_offset: int
    main_page_count: int
    date_setting: str
    confidential: bool
    dummy: bool


def _create_tex_footer_string(config: TocTexConfig):
    """Generate the footer string for the TeX TOC."""
    # pylint: disable=R0912,R0915
    bundle_logger.debug("[CTP]Creating TOC PDF. Parsing settings:")

    index_font_family = None
    footer_font = None
    starting_page = 1
    footer_alignment_setting = None
    footer_text = None
    if not config.roman_numbering:
        # parse index font setting
        # set starting page to be one more than the length_of_coversheet
        starting_page = config.length_of_coversheet + 1
        index_font_family = get_index_font_family(config.bundle_config.index_font)
        footer_alignment_setting = get_footer_alignment_setting(config.bundle_config.page_num_align)
        footer_font = get_footer_font(config.bundle_config.footer_font)
        footer_text = get_footer_text(
            config.bundle_config.footer_prefix, config.bundle_config.page_num_style, config.main_page_count, config.frontmatter_offset
        )

    return footer_alignment_setting, footer_font, footer_text, index_font_family, starting_page


def _initialize_bundle_creation(bundle_config_data: BundleConfig, output_file, coversheet, input_files, index_file):
    """Initialize variables and logging for bundle creation. Returns a list of initial temp files."""
    BUNTOOL_VERSION = "2025.01.24"

    # various initial file and data handling:
    bundle_config = bundle_config_data
    temp_dir = bundle_config.temp_dir
    temp_path = Path(temp_dir)
    temp_path.mkdir(parents=True, exist_ok=True)
    tmp_output_file = temp_dir / output_file
    coversheet_path = temp_dir / coversheet if coversheet else None

    # set up logging using configure_logger function
    bundle_logger = configure_logger(bundle_config, bundle_config.session_id)
    log_msg = f"""
        [CB]THIS IS BUNTOOL VERSION {BUNTOOL_VERSION}
        [CB]Temp directory created at {temp_dir}.
        *****New session: {bundle_config.session_id} called create_bundle*****
        {bundle_config.session_id} has the USER AGENT: {bundle_config.user_agent}
        Bundle creation called with {len(input_files)} input files and output file {output_file}"""
    bundle_logger.info(textwrap.dedent(log_msg).strip())
    debug_log_msg = f"""
            [CB]create_bundle received the following arguments:
            ....input_files: {input_files}
            ....output_file: {output_file}
            ....coversheet: {coversheet}
            ....index_file: {index_file}
            ....bundle_config: {bundle_config.__dict__}"""
    dedent_and_log(bundle_logger, debug_log_msg)

    initial_temp_files = [f for f in (coversheet_path, index_file) if f]

    return bundle_config, temp_path, tmp_output_file, coversheet_path, initial_temp_files + input_files


def _process_index_and_merge(bundle_config: BundleConfig, index_file, temp_path: Path, input_files):
    """Process index data and merge input PDFs. Returns index data, toc entries, and a list of temp files."""
    if not index_file and bundle_config.csv_string:
        index_file_path = temp_path / "index.csv"
        with Path(index_file_path).open("w") as f:
            f.write(bundle_config.csv_string)
        index_file = index_file_path
        bundle_logger.info(f"[CB]Index data from string input saved to {index_file}")

    if index_file:  # this is a file handler. The main way to pass an index.
        bundle_logger.debug(f"[CB]Calling load_index_data [LI] with index_file: {index_file}")
        index_data = load_index_data(index_file, bundle_config)
    else:
        index_data = {}
        bundle_logger.info("[CB]No index data provided.")

    # Merge PDFs using provided unique filenames
    merged_file = temp_path / "TEMP01_mainpages.pdf"
    log_msg = f"""
        [CB]Calling merge_pdfs_create_toc_entries [MP] with arguments:
        ....input_files: {input_files}
        ....merged_file: {merged_file}
        ....index_data: {index_data}"""
    dedent_and_log(bundle_logger, log_msg)
    try:
        toc_entries = merge_pdfs_create_toc_entries(input_files, merged_file, index_data)
    except Exception:
        bundle_logger.exception("[CB]Error while merging pdf files")
        raise
    else:
        if not Path(merged_file).exists():
            bundle_logger.info(f"[CB]Merging file unsuccessful: cannot locate expected ouput {merged_file}.")
            return None, None, [], None

        bundle_logger.info(f"[CB]Merged PDF created at {merged_file}")

    # list out settings in a human-readable way for remote user support.
    file_details = "\n".join(
        f'            ..File {idx + 1}: Filename "{file}"\n'
        f"            .... had index data: {file.name in index_data}\n"
        f"            .... had {len(Pdf.open(file).pages)} page(s)."
        for idx, file in enumerate(input_files)
    )

    user_settings_log = f"""\
        =============================================================================
        BUNTOOL -- BEGIN RECORD OF USER SETTINGS
        Time of use: {bundle_config.timestamp}
        STEP ONE:
        ..Bundle Title: {bundle_config.case_details.get('bundle_title', '')}
        ..Case Name: {bundle_config.case_details.get('case_name', '')}
        ..Claim Number: {bundle_config.case_details.get('claim_no', '')}
        STEP TWO:
        {file_details}
        STEP THREE:
        ..Index Options:
        ....Index font: {bundle_config.index_font}
        ....Coversheet: {"Yes" if bundle_config.case_details.get("case_name", '') else "No coversheet provided."}
        ....Date column: {bundle_config.date_setting}
        ....Confidentiality: {bundle_config.confidential_bool}
        ..Page Numbering Options:
        ....Footer font: {bundle_config.footer_font}
        ....Preface numbering: {bundle_config.roman_for_preface}
        ....Page number alignment: {bundle_config.page_num_align}
        ....Page numbering style: {bundle_config.page_num_style}
        ....Footer prefix: {bundle_config.footer_prefix if bundle_config.footer_prefix else "No footer prefix"}
        END RECORD OF USER SETTINGS
        ================================================================================="""
    dedent_and_log(bundle_logger, user_settings_log)

    with Pdf.open(merged_file) as mergedfile:
        main_page_count = len(mergedfile.pages)
    bundle_config.main_page_count = main_page_count  # main page count for x of y pagination if needed
    return index_data, toc_entries, [merged_file], main_page_count


def _create_front_matter(bundle_config, coversheet, coversheet_path, temp_path: Path, toc_entries):
    """Create and merge front matter (coversheet and TOC). Returns any temporary files created."""
    if coversheet and coversheet_path and Path(coversheet_path).exists():
        with Pdf.open(coversheet_path) as coversheet_pdf:
            length_of_coversheet = len(coversheet_pdf.pages)
    else:
        length_of_coversheet = 0

    length_of_dummy_toc = 0
    bundle_config.expected_length_of_frontmatter = length_of_coversheet  # global. This allows the toc to account for what comes before it.

    temp_files = []

    # First pass to create a dummy TOC to find the length of the frontmatter:
    if not bundle_config.roman_for_preface:
        bundle_logger.debug("[CB]Creating dummy TOC PDF to find length of frontmatter")
        try:
            dummy_toc_pdf_path = temp_path / "TEMP02_dummy_toc.pdf"
            options = {"confidential": bundle_config.confidential_bool, "date_setting": bundle_config.date_setting, "dummy": True}
            create_toc_pdf_reportlab(  # DUMMY TOC)
                toc_entries, bundle_config.case_details, bundle_config, dummy_toc_pdf_path, options
            )
        except Exception:
            bundle_logger.exception("[CB]Error during first pass TOC creation")
            raise
        if not Path(dummy_toc_pdf_path).exists():
            bundle_logger.error(f"[CB]First pass TOC file unsuccessful: cannot locate expected ouput {dummy_toc_pdf_path}.")
            return None, None, None, []
        bundle_logger.info(f"[CB]dummy TOC PDF created at {dummy_toc_pdf_path}")
        tempdir_path = temp_path
        temp_files.extend(
            [
                dummy_toc_pdf_path,
                tempdir_path / "dummytoc.out",
                tempdir_path / "TEMP02_dummy_toc.aux",
                tempdir_path / "dummytoc.tex",
            ]
        )
        # find length of dummy TOC:
        with Pdf.open(dummy_toc_pdf_path) as dummytocpdf:
            length_of_dummy_toc = len(dummytocpdf.pages)
            expected_length_of_frontmatter = length_of_coversheet + length_of_dummy_toc
    else:
        expected_length_of_frontmatter = length_of_coversheet

    bundle_config.total_number_of_pages = bundle_config.main_page_count + expected_length_of_frontmatter

    bundle_config.expected_length_of_frontmatter = expected_length_of_frontmatter  # global
    bundle_logger.debug(f"[CB]Expected length of frontmatter: {expected_length_of_frontmatter}")
    return expected_length_of_frontmatter, length_of_coversheet, length_of_dummy_toc, temp_files


class TocParams(NamedTuple):
    bundle_config: BundleConfig
    temp_path: Path
    toc_entries: list
    length_of_coversheet: int | None
    expected_length_of_frontmatter: int
    toc_file_path: Path


class CreateTocError(Exception):
    def __init__(self, message="TOC PDF creation failed."):
        super().__init__(message)

def _create_toc(toc_params: TocParams):
    (
        bundle_config,
        temp_path,
        toc_entries,
        length_of_coversheet,
        expected_length_of_frontmatter,
        toc_file_path
    ) = toc_params

    bundle_config.expected_length_of_frontmatter = length_of_coversheet if length_of_coversheet is not None else 0  # janky reset for TOC

    # Now, create TOC PDF For real:
    log_msg = f"""
        [CB]Calling create_toc_pdf_reportlab [CT] - final version -  with arguments:
        ....toc_entries: {toc_entries}
        ....casedetails: {bundle_config.case_details}
        ....toc_file_path: {toc_file_path}
        ....confidential: {bundle_config.confidential_bool}
        ....date_setting: {bundle_config.date_setting}
        ....index_font: {bundle_config.index_font}
        ....dummy: False
        ....length_of_frontmatter: {expected_length_of_frontmatter}"""
    dedent_and_log(bundle_logger, log_msg)
    options = {
        "confidential": bundle_config.confidential_bool,
        "date_setting": bundle_config.date_setting,
        "dummy": False,
        "roman_numbering": bundle_config.roman_for_preface,
    }
    create_toc_pdf_reportlab(toc_entries, bundle_config.case_details, bundle_config=bundle_config, output_file=toc_file_path, options=options)
    if not Path(toc_file_path).exists():
        raise CreateTocError()

    docx_output_path = None
    try:
        docx_output_path = temp_path / "docx_output.docx"
        docx_config = DocxConfig(
            confidential=bundle_config.confidential_bool,
            date_setting=(bundle_config.date_setting != "hide_date"),
            index_font_setting=bundle_config.index_font,
        )
        case_details_list = [
            bundle_config.case_details.get('bundle_title', ''),
            bundle_config.case_details.get('claim_no', ''),
            bundle_config.case_details.get('case_name', '')
        ]
        create_toc_docx(toc_entries, case_details_list, docx_output_path, docx_config)
    except Exception:
        bundle_logger.exception("[CB]..Error during create_toc_docx")
    return docx_output_path


def create_toc_pdf_tex(toc_entries, casedetails, output_file, config: TocTexConfig):
    """Generate a table of contents PDF using TeX."""
    bundle_name = sanitise_latex(casedetails[0])
    page_offset = 0 if config.dummy else config.frontmatter_offset + 1
    date_col_hdr, date_col_width = ("Date", "3.5cm") if config.date_setting != "hide_date" else ("", "0.3cm")
    claimno_hdr = sanitise_latex(casedetails[1]) if casedetails[1] else ""
    casename = sanitise_latex(casedetails[2]) if casedetails[2] else ""
    footer_alignment_setting, footer_font, footer_text, index_font_family, starting_page = _create_tex_footer_string(config)

    # Define the helper function for non-roman numbering
    def get_non_roman_pagestyle():
        return f"""
        \\newcommand{{\\fontsetting}}{{\\fontfamily{{{footer_font}}}\\fontseries{{b}}\\base_font_size{{18}}{{22}}\\selectfont}}
        \\setcounter{{page}}{{{starting_page}}}
        \\begin{{document}}
        \\pagestyle{{fancy}}
        \\renewcommand{{\\headrulewidth}}{{0pt}}
        \\setlength{{\\footskip}}{{20pt}}
        \\fancyhf{{}} % to clear the header and the footer simultaneously
        \\fancyfoot[{footer_alignment_setting}]{{\\fontsetting {footer_text}}}
        """

    # Define the bundle name header
    bundle_header = (
        f'\\textbf{{\\large{{\\textcolor{{red}}{{"CONFIDENTIAL"}}\\\\ {bundle_name.upper()}}}}} \\\\'
        if config.confidential
        else f"\\textbf{{\\Large{{{bundle_name.upper()}}}}} \\\\"
    )

    # Build all the parts of the LaTeX document
    parts = [
        r"""
\documentclass[12pt,a4paper]{article}
\usepackage{fancyhdr}
\usepackage{geometry}
\usepackage{hyperref}
\usepackage{longtable}
\usepackage{color, colortbl}
""",
        r"\usepackage{xcolor}" if config.confidential else "",
        r"""
\geometry{a4paper, hmargin=2.5cm,vmargin=2cm}
\definecolor{Gray}{gray}{0.9}
""",
        get_non_roman_pagestyle()
        if not config.roman_numbering
        else r"""
\begin{document}
\pagestyle{empty}
""",
        rf"\fontfamily{{{index_font_family}}}\selectfont" if index_font_family else "",
        rf"\hfill\textbf{{\normalsize{{{claimno_hdr}}}}} \\" if claimno_hdr else "",
        r"\vspace{{-0.5cm}}" if claimno_hdr else "",
        rf"""
\begin{{center}}
\textbf{{\large{{{casename}}}}} \\
\end{{center}}
"""
        if casename
        else "",
        rf"""
\begin{{center}}
\rule{{0.5\linewidth}}{{0.3mm}} \\
\vspace{{0.3cm}}
{bundle_header}
\rule{{0.5\linewidth}}{{0.3mm}} \\
\vspace{{-0.5cm}}
\end{{center}}
"""
        if bundle_name
        else "",
        rf"""
\def\arraystretch{{1.3}}
\begin{{longtable}}{{p{{1.2cm}} p{{10cm}} p{{{date_col_width}}} r}}
\hline
\textbf{{Tab}} & \textbf{{Title}} & \textbf{{{date_col_hdr}}} & \textbf{{Page}} \\
\hline
\endfirsthead
\hline
\textbf{{Tab}} & \textbf{{Title}} & \textbf{{{date_col_hdr}}} & \textbf{{Page}} \\
\hline
\endhead
\hline
\endfoot
\hline
\endlastfoot
""",
        "".join(
            [
                r"\hline \rowcolor{Gray}\multicolumn{4}{l}{\textbf{" + entry[1] + r"}} \\ \hline "
                if "SECTION_BREAK" in entry[0]
                else (
                    f"{sanitise_latex(entry[0])} & "
                    f"{sanitise_latex(entry[1])} & "
                    f"{'' if config.date_setting == 'hide_date' else sanitise_latex(entry[2])} & "
                    f"{999 if config.dummy else entry[3] + page_offset} \\\\"
                )
                for entry in toc_entries
            ]
        ),
        r"""
\end{longtable}
\newpage
\pagenumbering{arabic}
\end{document}
""",
    ]

    # Join all parts, filtering out empty strings
    toc_content = "".join(part for part in parts if part)

    # Determine output paths
    if config.dummy:
        toc_tex_path = Path(output_file).parent / "dummytoc.tex"
        jobname = "dummytoc"
    else:
        jobname = "toc"
        toc_tex_path = Path(output_file).parent / "toc.tex"

    # Write the LaTeX content to file
    with Path(toc_tex_path).open("w") as f:
        f.write(toc_content)

    # Compile the LaTeX document
    if Path(toc_tex_path).exists():
        bundle_logger.debug(f"[CTP]TOC content written to file: {toc_tex_path}")
        result = os.system(f"pdflatex -output-directory {Path(output_file).parent} -jobname={jobname} {toc_tex_path} > /dev/null")
        if result != 0:
            bundle_logger.error(f"[CTP]..pdflatex command failed with error code {result}")
        else:
            bundle_logger.debug("[CTP]..pdflatex command succeeded.")
    else:
        bundle_logger.error(f"[CTP]Error writing TOC content to file: {toc_tex_path}")


class BundleLastLegParams(NamedTuple):
    merged_file_with_frontmatter: Path
    length_of_coversheet: int | None
    bundle_config: BundleConfig
    temp_dir: Path
    hyperlinked_file: Path
    main_bookmarked_file: Path
    index_bookmarked_file: Path
    coversheet_path: Path | None
    frontmatter_path: Path
    length_of_frontmatter: int
    toc_entries: list
    tmp_output_file: Path

class HyperlinkingError(Exception):
    details: str
    def __init__(self, option, details):
        self.details = details
        super().__init__(f"Hyperlinking process failed: {option}")

class BookmarkingError(Exception):
    def __init__(self, option, details):
        self.details = details
        super().__init__(f"Bookmarking process failed: {option}")

class PageLabelsError(Exception):
    def __init__(self, option, details):
        self.details = details
        super().__init__(f"Page labels process failed: {option}")

def bundle_last_leg(bundle_last_leg_params: BundleLastLegParams):
    (
        merged_file_with_frontmatter,
        length_of_coversheet,
        bundle_config,
        _,  # temp_dir is not used in this function
        hyperlinked_file,
        main_bookmarked_file,
        index_bookmarked_file,
        coversheet_path,
        frontmatter_path,
        length_of_frontmatter,
        toc_entries,
        tmp_output_file,
    ) = bundle_last_leg_params

    bundle_logger.debug("[[CB]Beginning hyperlinking process")

    log_msg = f"""
        [CB]..Calling add_hyperlinks [AH] with arguments:
        ......merged_file_with_frontmatter: {merged_file_with_frontmatter}
        ......hyperlinked_file: {hyperlinked_file}
        ......length_of_coversheet: {length_of_coversheet}, length_of_frontmatter: {length_of_frontmatter}
        ......toc_entries: {toc_entries}
        ......date_setting: {bundle_config.date_setting}, roman_for_preface: {bundle_config.roman_for_preface}"""
    dedent_and_log(bundle_logger, log_msg)
    try:
        add_hyperlinks(
                merged_file_with_frontmatter,
                hyperlinked_file,
                length_of_coversheet if length_of_coversheet is not None else 0,
                length_of_frontmatter,
                toc_entries
            )
    except Exception as e:
        raise HyperlinkingError("A", "[CB]..Error during add_hyperlinks") from e
    if not Path(hyperlinked_file).exists():
        raise HyperlinkingError("B", f"[CB]..Hyperlinking file unsuccessful: cannot locate expected ouput {hyperlinked_file}.")


    log_msg = f"""
    [CB]Calling add_bookmarks_to_pdf [AB] with arguments:
    ....hyperlinked_file: {hyperlinked_file}
    ....main_bookmarked_file: {main_bookmarked_file}
    ....toc_entries: {toc_entries}
    ....length_of_frontmatter: {length_of_frontmatter}"""
    dedent_and_log(bundle_logger, log_msg)
    try:
        add_bookmarks_to_pdf(hyperlinked_file, main_bookmarked_file, toc_entries, length_of_frontmatter, bundle_config)
    except Exception as e:
        raise BookmarkingError("A", "[CB]..Error during add_bookmarks_to_pdf") from e
    if not Path(main_bookmarked_file).exists():
        raise BookmarkingError("B", f"[CB]..Bookmarking file unsuccessful: cannot locate expected ouput {main_bookmarked_file}.")

    log_msg = f"""
        [CB]Calling bookmark_the_index [BI] with arguments:
        ....main_bookmarked_file: {main_bookmarked_file}
        ....index_bookmarked_file: {index_bookmarked_file}
        ....coversheet_path: {coversheet_path}"""
    dedent_and_log(bundle_logger, log_msg)
    try:
        bookmark_the_index(main_bookmarked_file, index_bookmarked_file, coversheet_path)
    except Exception as e:
        raise BookmarkingError("C", "[CB]..Error during bookmark_the_index") from e
    if not Path(index_bookmarked_file).exists():
        raise BookmarkingError("D", f"[CB]..Bookmarking index file unsuccessful: cannot locate expected ouput {index_bookmarked_file}.")

    if bundle_config.roman_for_preface:
        log_msg = f"""
            [CB]Calling add_roman_labels [APL] with arguments:
            ....index_bookmarked_file: {index_bookmarked_file}
            ....frontmatter_path: {frontmatter_path}
            ....tmp_output_file: {tmp_output_file}"""
        dedent_and_log(bundle_logger, log_msg)
        try:
            add_roman_labels(index_bookmarked_file, length_of_frontmatter, tmp_output_file)
        except Exception as e:
            raise PageLabelsError("A", "[CB]..Error during add_roman_labels") from e
        if not Path(tmp_output_file).exists():
            raise PageLabelsError("B", f"[CB]..Adding page labels unsuccessful: cannot locate expected ouput {tmp_output_file}.")
        bundle_logger.info(f"[CB]..Page labels added to PDF saved to {tmp_output_file}")
    else:
        shutil.copyfile(index_bookmarked_file, tmp_output_file)

    bundle_logger.info(f"[CB]Completed bundle creation. output written to: {tmp_output_file}")

class AssembleFinalBundleParams(NamedTuple):
    bundle_config: BundleConfig
    temp_path: Path
    merged_file: Path
    expected_length_of_frontmatter: int
    toc_entries: list
    length_of_coversheet: int | None
    length_of_dummy_toc: int | None
    coversheet: bool
    coversheet_path: Path | None
    tmp_output_file: Path

class PathsTuple(NamedTuple):
    merged_paginated_no_toc: Path
    page_numbers_pdf: Path
    page_numbers_aux: Path
    page_numbers_tex: Path
    toc_file_path: Path
    toc_out: Path
    toc_log: Path
    toc_aux: Path
    toc_tex: Path
    merged_file_with_frontmatter: Path
    hyperlinked_file: Path
    main_bookmarked_file: Path
    index_bookmarked_file: Path

def get_paths(temp_path: Path):
    # Define all potential temporary file paths upfront
    merged_paginated_no_toc = temp_path / "TEMP03_paginated_mainpages.pdf"
    page_numbers_pdf = temp_path / "pageNumbers.pdf"
    page_numbers_aux = temp_path / "pageNumbers.aux"
    page_numbers_tex = temp_path / "pageNumbers.tex"
    toc_file_path = temp_path / "index.pdf"
    toc_out = temp_path / "index.out"
    toc_log = temp_path / "index.log"
    toc_aux = temp_path / "index.aux"
    toc_tex = temp_path / "toc.tex"
    merged_file_with_frontmatter = temp_path / "TEMP04_all_pages.pdf"
    hyperlinked_file = temp_path / "TEMP05-hyperlinked.pdf"
    main_bookmarked_file = temp_path / "TEMP06_main_bookmarks.pdf"
    index_bookmarked_file = temp_path / "TEMP07_all_bookmarks.pdf"
    return PathsTuple(merged_paginated_no_toc, page_numbers_pdf, page_numbers_aux, page_numbers_tex, toc_file_path, toc_out, toc_log, toc_aux,
        toc_tex, merged_file_with_frontmatter, hyperlinked_file, main_bookmarked_file, index_bookmarked_file)

class PaginationError(Exception):
    """Custom exception for pagination errors."""

    def __init__(self, option):
        self.message = f"Pagination process failed: {option}"
        super().__init__(self.message)

def paginate_merged_main_files(merged_file, merged_paginated_no_toc, bundle_config: BundleConfig):
    # Next step: paginate the merged main files of the PDF (the main content)
    log_msg = f"""
        [CB]Calling pdf_paginator_reportlab [PPRL] with arguments:
        ....merged_file: {merged_file}
        ....merged_paginated_no_toc: {merged_paginated_no_toc}
        ....page_num_alignment: {bundle_config.page_num_align}
        ....page_num_font: {bundle_config.footer_font}
        ....page_numbering_style: {bundle_config.page_num_style}
        ....footer_prefix: {bundle_config.footer_prefix}"""
    dedent_and_log(bundle_logger, log_msg)
    try:
        paginated_page_count = pdf_paginator_reportlab(merged_file, bundle_config, merged_paginated_no_toc)
    except Exception as e:
        bundle_logger.exception("[CB]..Error during pdf_paginator_reportlab")
        paginated_page_count = 0
        raise PaginationError("A") from e
    if not Path(merged_paginated_no_toc).exists():
        bundle_logger.error(f"[CB]..Paginating file unsuccessful: cannot locate expected ouput {merged_paginated_no_toc}.")
        raise PaginationError("B")

    assert paginated_page_count == bundle_config.main_page_count

class FrontMatterError(Exception):
    details: str
    def __init__(self, option, details):
        self.details = details
        super().__init__(f"Frontmatter process failed: {option}")

class SaveMergedFilesWithFrontmasterParams(NamedTuple):
    temp_path: Path
    toc_file_path: Path
    coversheet: bool
    coversheet_path: Path | None
    bundle_config: BundleConfig
    expected_length_of_frontmatter: int
    length_of_dummy_toc: int | None
    merged_paginated_no_toc: Path
    merged_file_with_frontmatter: Path

def save_merged_files_with_frontmaster(get_front_matter_path_params: SaveMergedFilesWithFrontmasterParams):
    (
        temp_path,
        toc_file_path,
        coversheet,
        coversheet_path,
        bundle_config,
        expected_length_of_frontmatter,
        length_of_dummy_toc,
        merged_paginated_no_toc,
        merged_file_with_frontmatter
    ) = get_front_matter_path_params

    frontmatter = temp_path / "TEMP00-coversheet-plus-toc.pdf"
    if coversheet:
        if coversheet_path and Path(coversheet_path).exists():
            frontmatterfiles = [coversheet_path, toc_file_path]
            log_msg = f"""
                [CB]Coversheet specified. Calling merge_frontmatter [MF] with arguments:
                ....frontmatterfiles: {frontmatterfiles}, frontmatter: {frontmatter}"""
            dedent_and_log(bundle_logger, log_msg)
            frontmatter_path = merge_frontmatter(frontmatterfiles, frontmatter)
            if not Path(frontmatter_path).exists():
                raise FrontMatterError("A", f"[CB]..Merging frontmatter unsuccessful: cannot locate expected ouput {frontmatter_path}.")
            bundle_logger.info(f"[CB]..Frontmatter created at {Path(frontmatter_path).name}")
        else:
            raise FrontMatterError("B", f"[CB]..Coversheet specified but not found at {coversheet_path}.")
    else:
        frontmatter_path = toc_file_path
        bundle_logger.info("[CB]No coversheet specified. TOC is the only frontmatter.")

    with Pdf.open(frontmatter_path) as frontmatter_pdf:
        length_of_frontmatter = len(frontmatter_pdf.pages)
        bundle_logger.debug(f"[CB]Frontmatter length is {length_of_frontmatter} pages.")
        if not bundle_config.roman_for_preface:
            if length_of_frontmatter != expected_length_of_frontmatter:
                error_msg = (
                    f"[CB]..Frontmatter length mismatch: expected {length_of_frontmatter} pages, "
                    f"got {expected_length_of_frontmatter}."
                )
                raise FrontMatterError("C", error_msg)
            bundle_logger.info(f"[CB]..Frontmatter length matches expected {length_of_dummy_toc} pages.")

    with Pdf.open(frontmatter_path) as frontmatter_pdf, Pdf.open(merged_paginated_no_toc) as main_pdf:
        merged_pdf = Pdf.new()
        merged_pdf.pages.extend(frontmatter_pdf.pages)
        merged_pdf.pages.extend(main_pdf.pages)
        merged_pdf.save(merged_file_with_frontmatter)
    if not Path(merged_file_with_frontmatter).exists():
        bundle_logger.exception(
            f"[CB]..Merging frontmatter with main docs unsuccessful: cannot locate expected ouput {merged_file_with_frontmatter}."
        )
        raise FrontMatterError("D", \
                            f"[CB]..Merging frontmatter with main docs unsuccessful: cannot locate expected ouput {merged_file_with_frontmatter}.")
    bundle_logger.info(f"[CB]..Merged frontmatter with main docs at {merged_file_with_frontmatter}")
    return frontmatter_path, length_of_frontmatter

def _assemble_final_bundle(
    assemble_final_bundle_params: AssembleFinalBundleParams,
):
    """Assembles the final PDF bundle by paginating, creating TOC, and adding bookmarks. Returns a list of created temp files."""
    (
        bundle_config,
        temp_dir,
        merged_file,
        expected_length_of_frontmatter,
        toc_entries,
        length_of_coversheet,
        length_of_dummy_toc,
        coversheet,
        coversheet_path,
        tmp_output_file
    ) = assemble_final_bundle_params

    temp_path = Path(temp_dir)

    (   merged_paginated_no_toc,
        page_numbers_pdf,
        page_numbers_aux,
        page_numbers_tex,
        toc_file_path,
        toc_out,
        toc_log,
        toc_aux,
        toc_tex,
        merged_file_with_frontmatter,
        hyperlinked_file,
        main_bookmarked_file,
        index_bookmarked_file
    ) = get_paths(temp_path)

    try:
        paginate_merged_main_files(merged_file, merged_paginated_no_toc, bundle_config)
        docx_output_path = _create_toc(TocParams(
            bundle_config, temp_path, toc_entries, length_of_coversheet, expected_length_of_frontmatter, toc_file_path
        ))
    except Exception as e:
        if isinstance(e, CreateTocError):
            bundle_logger.exception(f"[CB]..Creating TOC file unsuccessful: cannot locate expected output {toc_file_path}.")
        elif isinstance(e, PaginationError):
            bundle_logger.exception("[CB]..paginate_merged_main_files failed.")
        return None

    try:
        frontmatter_path, length_of_frontmatter = save_merged_files_with_frontmaster(SaveMergedFilesWithFrontmasterParams(
            temp_path, toc_file_path, coversheet, coversheet_path, bundle_config, expected_length_of_frontmatter,
            length_of_dummy_toc, merged_paginated_no_toc, merged_file_with_frontmatter
        ))
    except FrontMatterError:
        bundle_logger.exception("CB..Saving merged files with frontmatter failed.")
        return None

    try:
        bundle_last_leg(BundleLastLegParams(
            merged_file_with_frontmatter=merged_file_with_frontmatter,
            length_of_coversheet=length_of_coversheet,
            bundle_config=bundle_config,
            temp_dir=temp_path,
            hyperlinked_file=hyperlinked_file,
            main_bookmarked_file=main_bookmarked_file,
            index_bookmarked_file=index_bookmarked_file,
            coversheet_path=coversheet_path,
            frontmatter_path=frontmatter_path,
            length_of_frontmatter=length_of_frontmatter,
            toc_entries=toc_entries,
            tmp_output_file=tmp_output_file))
    except Exception as e:
        if isinstance(e, HyperlinkingError):
            bundle_logger.exception("[CB]..Hyperlinking process failed.")
        elif isinstance(e, BookmarkingError):
            bundle_logger.exception("[CB]..Bookmarking process failed.")
        elif isinstance(e, PageLabelsError):
            bundle_logger.exception("[CB]..Page labeling process failed.")
        return None

    # Return the path to the docx and the list of all temp files created in this function
    return docx_output_path, [
        merged_paginated_no_toc,
        page_numbers_pdf,
        page_numbers_aux,
        page_numbers_tex,
        toc_file_path,
        toc_out,
        toc_log,
        toc_aux,
        toc_tex,
        merged_file_with_frontmatter,
        hyperlinked_file,
        main_bookmarked_file,
        index_bookmarked_file,
    ]


def create_bundle(input_files, output_file, coversheet, index_file, bundle_config_data: BundleConfig):
    """Create a bundle from input files and configuration."""
    docx_output_path = None
    toc_file_path = None

    bundle_config, temp_path, tmp_output_file, coversheet_path, initial_temp_files = _initialize_bundle_creation(
        bundle_config_data, output_file, coversheet, input_files, index_file
    )

    try:
        index_data, toc_entries, merge_temp_files, main_page_count = _process_index_and_merge(bundle_config, index_file, temp_path, input_files)
        if not merge_temp_files:
            return None, None

        expected_length_of_frontmatter, length_of_coversheet, length_of_dummy_toc, frontmatter_temp_files = _create_front_matter(
            bundle_config, coversheet, coversheet_path, temp_path, toc_entries
        )
        if expected_length_of_frontmatter is None:
            return None, None

        if toc_entries is None:
            bundle_logger.error("[CB]toc_entries is None, cannot assemble bundle.")
            return None, None

        result = _assemble_final_bundle(
            AssembleFinalBundleParams(bundle_config,
            temp_path,
            merge_temp_files[0],  # Pass the path to the merged file
            expected_length_of_frontmatter,
            toc_entries,
            length_of_coversheet,
            length_of_dummy_toc,
            coversheet,
            coversheet_path,
            tmp_output_file
            )
        )
        if result is None:
            bundle_logger.error("[CB].._assemble_final_bundle failed.")
            return None, None
        docx_output_path, final_bundle_temp_files = result

        # Combine all temporary files at the end
        created_temp_files = merge_temp_files + (frontmatter_temp_files or []) + (final_bundle_temp_files or [])

    except Exception:
        bundle_logger.exception("[CB]Error during create_bundle")
        raise

    # Create zip file if requested:
    zip_filepath = None
    if bundle_config.zip_bool:
        zip_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        bundletitleforfilename = bundle_config.case_details["bundle_title"] or "Bundle"
        casenameforfilename = bundle_config.case_details["case_name"] or ""
        bundle_logger.debug("[CB]Calling create_zip_file:")
        try:
            zip_filepath = create_zip_file(
                CreateZipFileParams(
                    bundletitleforfilename,
                    casenameforfilename,
                    zip_timestamp,
                    input_files,
                    index_file,
                    docx_output_path,
                    toc_file_path,
                    coversheet_path,
                    temp_path,
                    tmp_output_file,
                )
            )
        except Exception:
            bundle_logger.exception("[CB]..Error during create_zip_file")
            raise
        if not zip_filepath or not Path(zip_filepath).exists():
            bundle_logger.exception(f"[CB]..Creating zip file unsuccessful: cannot locate expected output {zip_filepath}.")
            # return tmp_output_file, None
        else:
            bundle_logger.info(f"[CB]..Zip file created at {Path(zip_filepath).name}")

    list_of_temp_files = initial_temp_files + created_temp_files
    remaining_files = remove_temporary_files(list_of_temp_files)
    if remaining_files:
        bundle_logger.info(f"[CB]..Remaining temporary files (will be deleted on next system flush): {remaining_files}")
    else:
        bundle_logger.info("[CB]..All temporary files deleted successfully.")

    final_zip_path = str(zip_filepath) if zip_filepath else None
    return str(tmp_output_file), final_zip_path


class CreateZipFileParams(NamedTuple):
    bundle_title: str
    case_name: str
    timestamp: str
    input_files: list
    csv_path: str
    docx_path: Path | None
    toc_path: Path | None
    coversheet_path: Path | None
    temp_path: Path
    tmp_output_file: str


def create_zip_file(create_zip_file_params: CreateZipFileParams):
    """Package up everything into a zip for the user's reproducibility and record keeping.

    for the user's reproducability and record keeping.
    """
    (
        bundle_title,
        case_name,
        timestamp,
        input_files,
        csv_path,
        docx_path,
        toc_path,
        coversheet_path,
        temp_dir,
        tmp_output_file
    ) = create_zip_file_params

    # int_zip_filepath = os.path.join(temp_dir, zip_filename)
    int_zip_filepath = Path(temp_dir) / f"{bundle_title}_{case_name}_{timestamp}.zip"
    bundle_logger.debug(f"[CZF]Creating zip file at {int_zip_filepath}")

    with zipfile.ZipFile(int_zip_filepath, "w") as zipf:
        # Add input files to a subdirectory
        for file in input_files:
            zipf.write(file, Path("input_files") / Path(file).name)
        # Add CSV index to the root directory
        if csv_path:
            zipf.write(csv_path, Path(csv_path).name)
        # Add TOC to the root directory
        if toc_path:
            zipf.write(toc_path, Path(toc_path).name)
        # Add coversheet to the root directory
        if docx_path:
            zipf.write(docx_path, Path(docx_path).name)
        if coversheet_path:
            zipf.write(coversheet_path, Path(coversheet_path).name)
        # Add outputfile (whole bundle) to the root directory
        if tmp_output_file and Path(tmp_output_file).exists():
            zipf.write(tmp_output_file, Path(tmp_output_file).name)
    return int_zip_filepath


def main():
    """Command-line usage.

    Mainly used for spot-testing during development. As such it is at present poorly tested and doesn't implement the full range
    of functionality from create_bundle.
    """
    parser = argparse.ArgumentParser(description="Merge PDFs with bookmarks and optional coversheet.")
    parser.add_argument("input_files", nargs="+", help="Input PDF files")
    parser.add_argument("-o", "--output_file", help="Output PDF file", default=None)
    parser.add_argument("-b", "--bundlename", help="Title of the bundle", default="Bundle")
    parser.add_argument("-c", "--casename", help="Name of case e.g. Smith v Jones & ors", default="")
    parser.add_argument("-n", "--claimno", help="Claim number", default="")
    parser.add_argument("-coversheet", help="Optional coversheet PDF file", default=None)
    parser.add_argument("-index", help="Optional CSV file with predefined index data", default=None)
    parser.add_argument("-csv_index", help="CSV index data as a string", default=None)
    parser.add_argument("-zip", help="Flag to indicate if a zip file should be created", action="store_true", default=False)
    parser.add_argument("-confidential", help="Flag to indicate if bundle is confidential", action="store_true", default=False)
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    input_files = args.input_files
    output_file = secure_filename(args.output_file) if args.output_file else secure_filename(f"{args.bundlename}-{timestamp}.pdf")
    coversheet = args.coversheet
    index_file = args.index
    csv_index = args.csv_index
    confidential_bool = args.confidential
    zip_bool = args.zip if args.zip else False

    bundle_config = BundleConfig(
        BundleConfigParams(
            timestamp=timestamp,
            case_details={"bundle_title": args.bundlename, "claim_no": args.claimno, "case_name": args.casename},
            csv_string=csv_index,
            confidential_bool=confidential_bool,
            zip_bool=zip_bool,
            session_id=timestamp,
            user_agent="CLI",
            page_num_align="centre",
            index_font="sans",
            footer_font="sans",
            page_num_style="page_x_of_y",
            footer_prefix="",
            date_setting="DD_MM_YYYY",
            roman_for_preface=False,
        )
    )

    create_bundle(
        input_files,
        output_file,
        coversheet,
        index_file,
        bundle_config,
    )


if __name__ == "__main__":
    main()
