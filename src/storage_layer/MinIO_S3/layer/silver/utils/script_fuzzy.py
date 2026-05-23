import pandas as pd
import polars as pl
import numpy as np
import re
from pathlib import Path
from rapidfuzz import process, fuzz

from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_data_from_silver
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds

OUTPUT_PATH = Path(__file__).parent / "clusters_review.csv"
SOURCE_COLUMN = "company_name"
# Cột dẫn xuất chỉ dùng để tính similarity. company_name gốc giữ nguyên trong output.
MATCHING_COLUMN = "matching_key"
SOURCE_TAG_COLUMN = "source_tag"
# Tag để phân biệt nguồn input khi review cluster.
TAG_UNMAPPED = "unmapped"
TAG_SEED_CANONICAL = "seed_canonical"
# Sites đang được crawl. Hardcode để fuzzy chạy được kể cả khi 1 site chưa có Silver data
# (reader trả None thì skip site đó).
SILVER_SITES = ["topcv", "vietnamworks", "itviec"]
SEED_FILE_NAME = "company_mapping.csv"

# Industry / sector stopwords: các từ mô tả LĨNH VỰC (không phải brand) làm token_set_ratio
# bị inflate. Strip chúng đi để brand token dominate điểm số.
# CHỈ áp dụng cho matching_key, KHÔNG đụng vào company_name gốc.
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


def apply_keyword_mappings(series: pd.Series) -> pd.Series:
    """
    Thay thế các cụm từ tiếng Việt đặc trưng thành brand/tên viết tắt phổ biến.
    Áp dụng trên chuỗi đã uppercase, dùng word boundary để tránh cắt nhầm.
    """
    s = series.copy()
    for pattern, replacement in MAPPING_KEYWORDS:
        s = s.str.replace(r'\b' + re.escape(pattern) + r'\b', replacement, regex=True)
    s = s.str.replace(r'\s+', ' ', regex=True).str.strip()
    return s


def build_matching_key(series: pd.Series) -> pd.Series:
    """
    Tạo matching_key bằng cách strip industry stopwords khỏi company_name (đã uppercase).
    Dùng \\b boundary để không cắt nhầm giữa từ. Pattern build từ list đã sort dài-trước.
    """
    pattern = r'\b(' + '|'.join(re.escape(w) for w in INDUSTRY_STOPWORDS) + r')\b'
    s = series.str.replace(pattern, ' ', regex=True)
    s = s.str.replace(r'\s+', ' ', regex=True).str.strip()
    return s


def load_unmapped_from_silver() -> pd.DataFrame:
    """
    Concat Silver của tất cả sites, filter is_mapped=0, lấy distinct company_name.
    Cần is_mapped column tồn tại trong Silver schema (do map_canonical_company tạo ra).
    """
    frames: list[pl.LazyFrame] = []
    for site in SILVER_SITES:
        lf = get_jobs_data_from_silver(site)
        if lf is not None:
            frames.append(lf.select([SOURCE_COLUMN, "is_mapped"]))

    if not frames:
        return pd.DataFrame(columns=[SOURCE_COLUMN])

    unmapped = (
        pl.concat(frames, how="diagonal_relaxed")
        .filter(pl.col("is_mapped") == 0)
        .select(SOURCE_COLUMN)
        .unique()
        .drop_nulls()
        .collect()
        .to_pandas()
    )
    return unmapped


def load_seed_canonicals() -> pd.DataFrame:
    """
    Lấy distinct canonical_name từ seed CSV (đã được fuzzy phân loại sẵn) để làm "anchor".
    Anchor giúp cluster mới gộp được vào canonical đã tồn tại thay vì tạo cluster cô lập.
    """
    seed = read_seeds(SEED_FILE_NAME)
    return (
        seed.select(pl.col("canonical_name").str.to_uppercase().str.strip_chars().alias(SOURCE_COLUMN))
        .unique()
        .drop_nulls()
        .to_pandas()
    )


def build_fuzzy_input() -> pd.DataFrame:
    """
    Trả về DataFrame [company_name, source_tag]. Nếu 1 name xuất hiện ở cả 2 nguồn,
    ưu tiên giữ tag seed_canonical để khi review biết đây là anchor đã được validate.
    """
    unmapped = load_unmapped_from_silver()
    unmapped[SOURCE_TAG_COLUMN] = TAG_UNMAPPED

    canonicals = load_seed_canonicals()
    canonicals[SOURCE_TAG_COLUMN] = TAG_SEED_CANONICAL

    combined = pd.concat([canonicals, unmapped], ignore_index=True)
    return combined.drop_duplicates(subset=[SOURCE_COLUMN], keep="first").reset_index(drop=True)


def main() -> None:
    df = build_fuzzy_input()
    if df.empty:
        print("No company names to cluster (silver empty + seed empty). Exiting.")
        return

    upper_names = df[SOURCE_COLUMN]
    mapped_names = apply_keyword_mappings(upper_names)
    df[MATCHING_COLUMN] = build_matching_key(mapped_names)

    # Nếu matching_key rỗng (toàn industry stopwords), fallback dùng company_name gốc
    # để không mất row trong quá trình so similarity.
    empty_mask = df[MATCHING_COLUMN].str.len() == 0
    df.loc[empty_mask, MATCHING_COLUMN] = df.loc[empty_mask, SOURCE_COLUMN]

    names = df[MATCHING_COLUMN].tolist()

    print(f"Computing similarity for {len(names)} names (threshold={THRESHOLD}, len_ratio>={LEN_RATIO_MIN})...")
    raw_cluster_ids = build_clusters(names, THRESHOLD, LEN_RATIO_MIN)

    df = df.assign(_raw_cluster=raw_cluster_ids)

    # Đánh số lại cluster_id liên tục từ 1, sắp theo cluster lớn trước để review dễ
    cluster_sizes = df["_raw_cluster"].value_counts()
    # Chỉ giữ cluster mixed (có cả unmapped và seed_canonical) hoặc cluster unmapped >=2.
    # Cluster toàn seed_canonical bỏ qua vì đã được validate trước đó.
    review_clusters = _select_review_clusters(df, cluster_sizes)

    cluster_id_map = {raw_id: new_id for new_id, raw_id in enumerate(review_clusters, start=1)}

    review_df = df[df["_raw_cluster"].isin(review_clusters)].copy()
    review_df["cluster_id"] = review_df["_raw_cluster"].map(cluster_id_map)
    review_df["cluster_size"] = review_df["_raw_cluster"].map(cluster_sizes)

    # Sort: seed_canonical lên đầu mỗi cluster làm anchor visual, kế đến matching_key
    review_df = review_df.sort_values(
        by=["cluster_size", "cluster_id", SOURCE_TAG_COLUMN, MATCHING_COLUMN],
        ascending=[False, True, True, True],
    )

    review_df = review_df[["cluster_id", "cluster_size", SOURCE_COLUMN, SOURCE_TAG_COLUMN, MATCHING_COLUMN]]

    review_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved {len(review_df)} rows in {len(review_clusters)} clusters -> {OUTPUT_PATH}")
    print(f"Singletons / pure-seed clusters skipped: {len(df) - len(review_df)}")


def _select_review_clusters(df: pd.DataFrame, cluster_sizes: pd.Series) -> list[int]:
    """
    Cluster đáng review: (a) mixed (có ít nhất 1 unmapped + 1 seed_canonical) hoặc
    (b) thuần unmapped với >= 2 members. Cluster thuần seed_canonical đã validate => skip.
    """
    cluster_tags = df.groupby("_raw_cluster")[SOURCE_TAG_COLUMN].agg(set)
    is_mixed = cluster_tags.apply(lambda tags: TAG_UNMAPPED in tags and TAG_SEED_CANONICAL in tags)
    is_pure_unmapped = cluster_tags.apply(lambda tags: tags == {TAG_UNMAPPED})
    multi_member = cluster_sizes >= 2
    keep_mask = (is_mixed) | (is_pure_unmapped & multi_member)
    return keep_mask[keep_mask].index.tolist()


if __name__ == "__main__":
    main()
