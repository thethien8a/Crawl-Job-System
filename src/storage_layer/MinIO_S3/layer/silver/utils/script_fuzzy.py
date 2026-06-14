import polars as pl
import numpy as np
import re
import logging
import argparse
from rapidfuzz import process, fuzz

from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_silver_by_site
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds
from src.storage_layer.MinIO_S3.layer.silver.utils.google_sheets import (
    get_or_create_worksheet,
    open_spreadsheet,
    replace_worksheet_values,
    worksheet_title_for_csv,
)

logger = logging.getLogger(__name__)

OUTPUT_SHEET_CSV_NAME = "clusters_review.csv"
SOURCE_COLUMN = "clean_company_name"
# Cột dẫn xuất chỉ dùng để tính similarity. clean_company_name gốc giữ nguyên trong output.
MATCHING_COLUMN = "matching_key"
HAS_IN_MAPPING_COLUMN = "has_in_company_mapping"
# Sites đang được crawl. Hardcode để fuzzy chạy được kể cả khi 1 site chưa có Silver data
# (reader trả None thì skip site đó).
SILVER_SITES = ["topcv", "vietnamworks", "itviec"]
SEED_FILE_NAME = "company_mapping.csv"
DEFAULT_ENTITY_NAME = "jobs"

# Industry / sector stopwords: các từ mô tả LĨNH VỰC (không phải brand) làm token_set_ratio
# bị inflate. Strip chúng đi để brand token dominate điểm số.
# CHỈ áp dụng cho matching_key, KHÔNG đụng vào clean_company_name gốc.
# Sắp cụm dài lên trước để regex ưu tiên match cụm dài.
INDUSTRY_STOPWORDS = [
    "ĐẦU TƯ THƯƠNG MẠI PHÁT TRIỂN CÔNG NGHỆ",
    "ĐẦU TƯ THƯƠNG MẠI PHÁT TRIỂN",
    "ĐẦU TƯ PHÁT TRIỂN CHUYỂN GIAO CÔNG NGHỆ",
    "ĐẦU TƯ PHÁT TRIỂN CÔNG NGHỆ",
    "ĐẦU TƯ PHÁT TRIỂN DỊCH VỤ",
    "ĐẦU TƯ PHÁT TRIỂN",
    "ĐẦU TƯ THƯƠNG MẠI",
    "ĐẦU TƯ CÔNG NGHỆ",
    "ĐẦU TƯ GIÁO DỤC",
    "ĐẦU TƯ KỸ THUẬT",
    "THƯƠNG MẠI DỊCH VỤ XUẤT NHẬP KHẨU",
    "THƯƠNG MẠI VÀ DỊCH VỤ",
    "THƯƠNG MẠI DỊCH VỤ",
    "DỊCH VỤ THƯƠNG MẠI",
    "XUẤT NHẬP KHẨU THƯƠNG MẠI",
    "XÂY DỰNG XUẤT NHẬP KHẨU",
    "XUẤT NHẬP KHẨU",
    "TÀI CHÍNH NGÂN HÀNG",
    "DỊCH VỤ CÔNG NGHỆ",
    "GIẢI PHÁP CÔNG NGHỆ BẤT ĐỘNG SẢN",
    "GIẢI PHÁP CÔNG NGHỆ",
    "CÔNG NGHỆ GIẢI PHÁP",
    "CÔNG NGHỆ THÔNG TIN",
    "CÔNG NGHỆ GIÁO DỤC",
    "CÔNG NGHỆ QUỐC TẾ",
    "CÔNG NGHỆ TRUYỀN THÔNG",
    "CÔNG NGHỆ DỮ LIỆU",
    "CÔNG NGHIỆP VIỄN THÔNG",
    "KHOA HỌC CÔNG NGHỆ",
    "PHÁT TRIỂN CÔNG NGHỆ",
    "GIẢI PHÁP PHẦN MỀM",
    "TƯ VẤN GIÁO DỤC",
    "TƯ VẤN QUẢN LÝ",
    "TƯ VẤN CÔNG NGHỆ",
    "TƯ VẤN DỊCH VỤ",
    "TƯ VẤN PHÂN TÍCH",
    "SẢN XUẤT KINH DOANH",
    "SẢN XUẤT THƯƠNG MẠI",
    "KỸ THUẬT THƯƠNG MẠI",
    "THƯƠNG MẠI VẬN TẢI",
    "BÁN LẺ KỸ THUẬT",
    "THIẾT BỊ CÔNG NGHIỆP",
    "VẬT LIỆU XÂY DỰNG",
    "TRUNG TÂM THỂ DỤC THỂ HÌNH",
    "DỊCH VỤ DU LỊCH TRẢI NGHIỆM",
    "DỊCH VỤ DU LỊCH",
    "DỊCH VỤ ĐỊA ỐC",
    "DỊCH VỤ BẤT ĐỘNG SẢN",
    "DỊCH VỤ DI ĐỘNG TRỰC TUYẾN",
    "DỊCH VỤ DI ĐỘNG",
    "CHUYỂN PHÁT NHANH",
    "CHUYỂN ĐỔI DOANH NGHIỆP",
    "PHÂN TÍCH XỬ LÝ DỮ LIỆU",
    "GIÁO DỤC ĐÀO TẠO",
    "GIÁO DỤC QUỐC TẾ",
    "TRƯỜNG ĐẠI HỌC CÔNG NGHỆ",
    "TRƯỜNG ĐẠI HỌC",
    "BỆNH VIỆN ĐA KHOA QUỐC TẾ",
    "BỆNH VIỆN ĐA KHOA",
    "BỆNH VIỆN",
    "HỆ THỐNG Y TẾ",
    "BẤT ĐỘNG SẢN",
    "TRUYỀN THÔNG",
    "HỆ THỐNG",
    "GIÁO DỤC",
    "TÀI CHÍNH",
    "VIỄN THÔNG",
    "BẢO HIỂM",
    "AN NINH MẠNG",
    "PHẦN MỀM",
    "VẬN TẢI",
    "XÂY DỰNG",
    "DU LỊCH",
    "THỜI TRANG",
    "THỰC PHẨM",
    "DƯỢC PHẨM",
    "VÀNG BẠC ĐÁ QUÝ",
    "NHÀ HÀNG",
    "KHÁCH SẠN",
    "ĐẦU TƯ",
    "PHÁT TRIỂN",
    "THƯƠNG MẠI",
    "DỊCH VỤ",
    "VIỆT NAM",
    "CÔNG NGHỆ",
    "QUỐC TẾ",
    "GIẢI PHÁP",
    "SẢN XUẤT",
    "KỸ THUẬT",
    "TƯ VẤN",
    "TRỰC TUYẾN",
    "DI ĐỘNG",
    "Y TẾ",
    "NGÂN HÀNG",
    "LOGISTICS",
    "CHỨNG KHOÁN",
    "KIỂM TOÁN",
    "KINH DOANH",
    "BÁN LẺ",
    "THIẾT BỊ",
    "MÁY CÔNG TRÌNH",
    "TM DV",
    "TM",
    "DV",
    "SX",
    "XNK",
    "BĐS",
    "CN",
]

MAPPING_KEYWORDS = [
    ("VIỄN THÔNG QUÂN ĐỘI", "VIETTEL"),
    ("BƯU CHÍNH VIỄN THÔNG", "VNPT"),
    ("GIẢI PHÁP THANH TOÁN", "VNPAY"),
    ("CÔNG THƯƠNG", "VIETINBANK"),
    ("ĐẦU TƯ VÀ PHÁT TRIỂN", "BIDV"),
    ("NGOẠI THƯƠNG", "VIETCOMBANK"),
    ("SÀI GÒN THƯƠNG TÍN", "SACOMBANK"),
    ("KỸ THƯƠNG", "TECHCOMBANK"),
    ("Á CHÂU", "ACB"),
    ("QUÂN ĐỘI", "MB BANK"),
    ("SỮA VIỆT NAM", "VINAMILK"),
    ("DẦU KHÍ", "PETROVIETNAM"),
    ("DẦU VIỆT NAM", "PETROLIMEX"),
    ("ĐIỆN LỰC", "EVN"),
    ("HÀNG KHÔNG", "VIETNAM AIRLINES"),
    ("VIỆT NAM AIRLINES", "VIETNAM AIRLINES"),
]

# Ngưỡng similarity, càng cao càng chính xác nhưng càng ít kết quả
THRESHOLD = 90

# Length-ratio guard: chống token_set_ratio fail-mode khi 1 chuỗi là subset của chuỗi kia
# (vd "FPT" vs "FPT TELECOM" = 100 → kéo theo chain merge thành supercluster).
# Yêu cầu chuỗi ngắn phải dài >= 60% chuỗi dài thì mới được gộp.
LEN_RATIO_MIN = 0.6


def build_clusters(names: list[str], threshold: int, len_ratio_min: float) -> list[int]:
    """
    Trả về list cluster_id cùng độ dài với names. Dùng union-find trên ma trận similarity
    để gộp các tên gần nhau thành 1 cluster.
    """
    n = len(names)
    # cdist trả về ma trận n x n các điểm similarity (0-100). token_set_ratio xử lý tốt
    # trường hợp thừa/thiếu từ ("LOTTE C F" vs "LOTTE C F VIETNAM" vs "LOTTE C AND F").
    score_matrix = process.cdist(names, names, scorer=fuzz.token_set_ratio, dtype=np.uint8)

    lengths = np.array([len(s) for s in names])

    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Chỉ duyệt nửa trên ma trận, bỏ đường chéo (i == j)
    for i in range(n):
        for j in range(i + 1, n):
            if score_matrix[i, j] < threshold:
                continue
            # Length guard: bỏ qua nếu 2 tên chênh lệch độ dài quá lớn
            short, long_ = sorted((lengths[i], lengths[j]))
            if long_ == 0 or short / long_ < len_ratio_min:
                continue
            union(i, j)

    return [find(i) for i in range(n)]


def apply_keyword_mappings(names: list[str]) -> list[str]:
    """
    Thay thế các cụm từ tiếng Việt đặc trưng thành brand/tên viết tắt phổ biến.
    Áp dụng trên chuỗi đã uppercase, dùng word boundary để tránh cắt nhầm.
    """
    mapped_names = []
    for name in names:
        mapped_name = name
        for pattern, replacement in MAPPING_KEYWORDS:
            mapped_name = re.sub(r'\b' + re.escape(pattern) + r'\b', replacement, mapped_name)
        mapped_names.append(re.sub(r'\s+', ' ', mapped_name).strip())
    return mapped_names


def build_matching_key(names: list[str]) -> list[str]:
    """
    Tạo matching_key bằng cách strip industry stopwords khỏi clean_company_name (đã uppercase).
    Dùng \\b boundary để không cắt nhầm giữa từ. Pattern build từ list đã sort dài-trước.
    """
    pattern = r'\b(' + '|'.join(re.escape(w) for w in INDUSTRY_STOPWORDS) + r')\b'
    return [re.sub(r'\s+', ' ', re.sub(pattern, ' ', name)).strip() for name in names]


def load_unique_company_names_from_silver(
    entity_name: str,
    from_date: str,
    to_date: str,
) -> pl.DataFrame:
    """
    Concat Silver của tất cả sites, lấy distinct clean_company_name để cluster.
    """
    frames: list[pl.LazyFrame] = []
    for site in SILVER_SITES:
        lf = get_jobs_silver_by_site(site, entity_name, from_date, to_date)
        if lf is not None:
            frames.append(lf.select(SOURCE_COLUMN))

    if not frames:
        return pl.DataFrame({SOURCE_COLUMN: []}, schema={SOURCE_COLUMN: pl.String})

    return (
        pl.concat(frames, how="diagonal_relaxed")
        .select(pl.col(SOURCE_COLUMN).str.to_uppercase().str.strip_chars())
        .unique()
        .drop_nulls()
        .collect()
    )


def load_seed_canonicals() -> pl.DataFrame:
    """
    Lấy distinct canonical_name từ seed CSV để flag name đã có trong mapping.
    """
    seed = read_seeds(SEED_FILE_NAME)
    return (
        seed.select(pl.col("canonical_name").str.to_uppercase().str.strip_chars().alias(SOURCE_COLUMN))
        .unique()
        .drop_nulls()
    )


def build_fuzzy_input(entity_name: str, from_date: str, to_date: str) -> pl.DataFrame:
    """
    Trả về DataFrame [clean_company_name, has_in_company_mapping] từ unique Silver names.
    """
    silver_names = load_unique_company_names_from_silver(entity_name, from_date, to_date)
    if silver_names.is_empty():
        return silver_names.with_columns(pl.lit(None).cast(pl.Int64).alias(HAS_IN_MAPPING_COLUMN))

    canonicals = load_seed_canonicals()
    return (
        silver_names.join(
            canonicals.with_columns(pl.lit(1).alias(HAS_IN_MAPPING_COLUMN)),
            on=SOURCE_COLUMN,
            how="left",
        )
        .with_columns(pl.col(HAS_IN_MAPPING_COLUMN).fill_null(0).cast(pl.Int64))
        .select(SOURCE_COLUMN, HAS_IN_MAPPING_COLUMN)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build fuzzy company-name clusters from Silver data.")
    parser.add_argument("--from_date", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--to_date", required=True, help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--entity_name", default=DEFAULT_ENTITY_NAME)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = build_fuzzy_input(args.entity_name, args.from_date, args.to_date)
    if df.is_empty():
        logger.info("No company names to cluster (silver empty). Exiting.")
        return

    upper_names = df.get_column(SOURCE_COLUMN).to_list()
    mapped_names = apply_keyword_mappings(upper_names)
    matching_keys = build_matching_key(mapped_names)

    # Nếu matching_key rỗng (toàn industry stopwords), fallback dùng clean_company_name gốc
    # để không mất row trong quá trình so similarity.
    matching_keys = [key or source for key, source in zip(matching_keys, upper_names)]
    df = df.with_columns(pl.Series(MATCHING_COLUMN, matching_keys))

    names = df.get_column(MATCHING_COLUMN).to_list()

    logger.info("Computing similarity for %d names (threshold=%d, len_ratio>=%f)...", len(names), THRESHOLD, LEN_RATIO_MIN)
    raw_cluster_ids = build_clusters(names, THRESHOLD, LEN_RATIO_MIN)

    df = df.with_columns(pl.Series("_raw_cluster", raw_cluster_ids))

    # Đánh số lại cluster_id liên tục từ 1, sắp theo cluster lớn trước để review dễ
    cluster_sizes = df.group_by("_raw_cluster").len().rename({"len": "cluster_size"})
    review_clusters = _select_review_clusters(df, cluster_sizes)

    cluster_id_map = {raw_id: new_id for new_id, raw_id in enumerate(review_clusters, start=1)}

    cluster_meta = cluster_sizes.with_columns(
        pl.col("_raw_cluster")
        .replace(cluster_id_map, default=None)
        .cast(pl.Int64)
        .alias("cluster_id")
    ).drop_nulls("cluster_id")

    review_df = df.join(cluster_meta, on="_raw_cluster", how="inner")

    # Ưu tiên tên đã có mapping lên đầu mỗi cluster để làm anchor visual.
    review_df = review_df.sort(
        by=["cluster_size", "cluster_id", HAS_IN_MAPPING_COLUMN, MATCHING_COLUMN],
        descending=[True, False, True, False],
    )

    review_df = review_df.select("cluster_id", "cluster_size", SOURCE_COLUMN, HAS_IN_MAPPING_COLUMN, MATCHING_COLUMN)

    worksheet_title = save_review_clusters_to_google_sheets(review_df)

    logger.info(
        "Saved %d rows in %d clusters -> Google Sheets worksheet '%s'",
        review_df.height,
        len(review_clusters),
        worksheet_title,
    )
    logger.info("Singleton clusters skipped: %d", df.height - review_df.height)


def save_review_clusters_to_google_sheets(review_df: pl.DataFrame) -> str:
    """
    Replace the clusters_review worksheet with the latest fuzzy cluster review output.
    """
    values = dataframe_to_sheet_values(review_df)
    worksheet_title = worksheet_title_for_csv(OUTPUT_SHEET_CSV_NAME)
    worksheet = get_or_create_worksheet(
        open_spreadsheet(),
        title=worksheet_title,
        rows=len(values),
        cols=max((len(row) for row in values), default=1),
    )
    replace_worksheet_values(worksheet, values)
    return worksheet_title


def dataframe_to_sheet_values(df: pl.DataFrame) -> list[list[str]]:
    """
    Convert a Polars DataFrame to rectangular values accepted by Google Sheets.
    """
    if not df.columns:
        return []

    return [
        df.columns,
        *[
            ["" if value is None else str(value) for value in row]
            for row in df.iter_rows()
        ],
    ]


def _select_review_clusters(df: pl.DataFrame, cluster_sizes: pl.DataFrame) -> list[int]:
    """
    Cluster đáng review: cluster có ít nhất 2 unique clean_company_name.
    """
    return (
        cluster_sizes
        .filter(pl.col("cluster_size") >= 2)
        .get_column("_raw_cluster")
        .to_list()
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()
