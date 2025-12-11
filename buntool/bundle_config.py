import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import NamedTuple


class BundleConfigParams(NamedTuple):
    timestamp: str
    case_details: dict[str, str]
    csv_string: str
    confidential_bool: bool
    zip_bool: bool
    session_id: str
    user_agent: str
    page_num_align: str
    index_font: str
    footer_font: str
    page_num_style: str
    footer_prefix: str
    date_setting: str
    roman_for_preface: bool
    expected_length_of_frontmatter: int | None = 0
    main_page_count: int = 0
    temp_dir: Path | None = None
    logs_dir: Path | None = None
    bookmark_setting: str = "uk_abbreviated"


@dataclass(init=False)
class BundleConfig:
    def __init__(
        self,
        bundle_config_params: BundleConfigParams,
    ):
        timestamp = bundle_config_params.timestamp
        case_details = bundle_config_params.case_details
        csv_string = bundle_config_params.csv_string
        confidential_bool = bundle_config_params.confidential_bool
        zip_bool = bundle_config_params.zip_bool
        session_id = bundle_config_params.session_id
        user_agent = bundle_config_params.user_agent
        page_num_align = bundle_config_params.page_num_align
        index_font = bundle_config_params.index_font
        footer_font = bundle_config_params.footer_font
        page_num_style = bundle_config_params.page_num_style
        footer_prefix = bundle_config_params.footer_prefix
        date_setting = bundle_config_params.date_setting
        roman_for_preface = bundle_config_params.roman_for_preface
        expected_length_of_frontmatter = bundle_config_params.expected_length_of_frontmatter
        main_page_count = bundle_config_params.main_page_count
        temp_dir = bundle_config_params.temp_dir
        logs_dir = bundle_config_params.logs_dir
        bookmark_setting = bundle_config_params.bookmark_setting

        self.timestamp = timestamp or datetime.now().strftime("%Y-%m-%d-%H%M%S")
        self.case_details = case_details
        self.csv_string = csv_string if csv_string else None
        self.confidential_bool = confidential_bool if confidential_bool else False
        self.zip_bool = zip_bool if zip_bool else True
        self.session_id = session_id if session_id else timestamp
        self.user_agent = user_agent or "Unknown"
        self.page_num_align = page_num_align if page_num_align else "centre"
        self.index_font = index_font if index_font else "Default"
        self.footer_font = footer_font if footer_font else "Default"
        self.page_num_style = page_num_style if page_num_style else "page_x_of_y"
        self.footer_prefix = footer_prefix if footer_prefix else ""
        self.date_setting = date_setting if date_setting else "DD_MM_YYYY"
        self.roman_for_preface = roman_for_preface if roman_for_preface else False
        self.expected_length_of_frontmatter = expected_length_of_frontmatter if expected_length_of_frontmatter else 0
        self.main_page_count = main_page_count
        self.total_number_of_pages = self.main_page_count + self.expected_length_of_frontmatter
        base_temp = tempfile.gettempdir()
        self.temp_dir = temp_dir if temp_dir else Path(base_temp) / "buntool" / "tempfiles" / self.session_id
        self.logs_dir = logs_dir if logs_dir else Path(base_temp) / "buntool" / "logs" / self.session_id
        self.bookmark_setting = bookmark_setting if bookmark_setting else "uk_abbreviated"
