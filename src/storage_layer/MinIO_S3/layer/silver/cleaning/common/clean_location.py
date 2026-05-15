import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_vietnamese_accents


VIETNAM_LOCATIONS = [
    # 5 Thành phố trực thuộc trung ương
    (r'(ho chi minh|hcm|sai gon|saigon|tphcm|tp\.hcm|hochiminh)', 'Hồ Chí Minh'),
    (r'(ha noi|hanoi|tp hn|tphn|tp\.hn)', 'Hà Nội'),
    (r'(da nang|danang|tp dn|tpdn|tp\.dn)', 'Đà Nẵng'),
    (r'(hai phong|haiphong|tp hp|tphp|tp\.hp)', 'Hải Phòng'),
    (r'(can tho|cantho|tp ct|tpct|tp\.ct)', 'Cần Thơ'),

    # Các tỉnh miền Bắc
    (r'(bac giang|bacgiang)', 'Bắc Giang'),
    (r'(bac kan|backan)', 'Bắc Kạn'),
    (r'(bac ninh|bacninh)', 'Bắc Ninh'),
    (r'(cao bang|caobang)', 'Cao Bằng'),
    (r'(dien bien|dienbien)', 'Điện Biên'),
    (r'(ha giang|hagiang)', 'Hà Giang'),
    (r'(ha nam|hanam)', 'Hà Nam'),
    (r'(hai duong|haiduong)', 'Hải Dương'),
    (r'(hoa binh|hoabinh)', 'Hòa Bình'),
    (r'(hung yen|hungyen)', 'Hưng Yên'),
    (r'(lai chau|laichau)', 'Lai Châu'),
    (r'(lang son|langson)', 'Lạng Sơn'),
    (r'(lao cai|laocai)', 'Lào Cai'),
    (r'(nam dinh|namdinh)', 'Nam Định'),
    (r'(ninh binh|ninhbinh)', 'Ninh Bình'),
    (r'(phu tho|phutho)', 'Phú Thọ'),
    (r'(quang ninh|quangninh)', 'Quảng Ninh'),
    (r'(son la|sonla)', 'Sơn La'),
    (r'(thai binh|thaibinh)', 'Thái Bình'),
    (r'(thai nguyen|thainguyen)', 'Thái Nguyên'),
    (r'(tuyen quang|tuyenquang)', 'Tuyên Quang'),
    (r'(vinh phuc|vinhphuc)', 'Vĩnh Phúc'),
    (r'(yen bai|yenbai)', 'Yên Bái'),

    # Các tỉnh miền Trung & Tây Nguyên
    (r'(ha tinh|hatinh)', 'Hà Tĩnh'),
    (r'(nghe an|nghean)', 'Nghệ An'),
    (r'(quang binh|quangbinh)', 'Quảng Bình'),
    (r'(quang tri|quangtri)', 'Quảng Trị'),
    (r'(thanh hoa|thanhhoa)', 'Thanh Hóa'),
    (r'(thua thien hue|hue)', 'Thừa Thiên Huế'),
    (r'(binh dinh|bindinh)', 'Bình Định'),
    (r'(binh thuan|binhthuan)', 'Bình Thuận'),
    (r'(khanh hoa|khanhhoa)', 'Khánh Hòa'),
    (r'(ninh thuan|ninhthuan)', 'Ninh Thuận'),
    (r'(phu yen|phuyen)', 'Phú Yên'),
    (r'(quang nam|quangnam)', 'Quảng Nam'),
    (r'(quang ngai|quangngai)', 'Quảng Ngãi'),
    (r'(dak lak|daklak)', 'Đắk Lắk'),
    (r'(dak nong|daknong)', 'Đắk Nông'),
    (r'(gia lai|gialai)', 'Gia Lai'),
    (r'(kon tum|kontum)', 'Kon Tum'),
    (r'(lam dong|lamdong)', 'Lâm Đồng'),

    # Các tỉnh miền Nam
    (r'(ba ria|vung tau|brvt|br\-vt)', 'Bà Rịa - Vũng Tàu'),
    (r'(binh duong|binhduong)', 'Bình Dương'),
    (r'(binh phuoc|binhphuoc)', 'Bình Phước'),
    (r'(dong nai|dongnai)', 'Đồng Nai'),
    (r'(tay ninh|tayninh)', 'Tây Ninh'),
    (r'(an giang|angiang)', 'An Giang'),
    (r'(bac lieu|baclieu)', 'Bạc Liêu'),
    (r'(ben tre|bentre)', 'Bến Tre'),
    (r'(ca mau|camau)', 'Cà Mau'),
    (r'(dong thap|dongthap)', 'Đồng Tháp'),
    (r'(hau giang|haugiang)', 'Hậu Giang'),
    (r'(kien giang|kiengiang)', 'Kiên Giang'),
    (r'(long an|longan)', 'Long An'),
    (r'(soc trang|socrang)', 'Sóc Trăng'),
    (r'(tien giang|tiengiang)', 'Tiền Giang'),
    (r'(tra vinh|travinh)', 'Trà Vinh'),
    (r'(vinh long|vinhlong)', 'Vĩnh Long'),
]


def remove_accents_col(df: pl.DataFrame, column_name: str, new_column_name: str) -> pl.DataFrame:
    """
    Hàm apply loại bỏ dấu tiếng Việt cho toàn bộ một cột trong Polars DataFrame.
    """
    return df.with_columns(
        pl.col(column_name)
        .cast(pl.String)
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .alias(new_column_name)
    )


def clean_location(df: pl.DataFrame, column_name: str = "location", new_column_name: str = "clean_location") -> pl.DataFrame:
    """
    Chuẩn hóa địa điểm về tên tỉnh/thành phố thống nhất.
    Sử dụng cột đã loại bỏ dấu để so khớp pattern dễ dàng hơn.
    """
    temp_col = f"{column_name}_no_accent"

    df = remove_accents_col(df, column_name=column_name, new_column_name=temp_col)

    title_lower = pl.col(temp_col).str.to_lowercase()

    location_expr = pl.lit(None, dtype=pl.String)
    for pattern, standard_name in VIETNAM_LOCATIONS:
        location_expr = pl.when(title_lower.str.contains(pattern)).then(pl.lit(standard_name)).otherwise(location_expr)

    location_expr = location_expr.otherwise(pl.lit("Không xác định"))

    df = df.with_columns(location_expr.alias(new_column_name))

    df = df.drop(temp_col)

    return df
