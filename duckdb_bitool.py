import streamlit as st
import pandas as pd
import plotly.express as px
from itertools import chain
import duckdb
import streamlit as st
import pandas as pd
import duckdb
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2 import service_account

import streamlit as st
import json
from google.oauth2 import service_account

# 🔺 Đặt lệnh set_page_config ở dòng đầu tiên
st.set_page_config(page_title="Dashboard TikTok BI Tool", layout="centered")

# ========================
# Fuctions cần thiết
# ========================


def clean_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = (
            df[col]
            .astype(str)  # chuyển sang string để xử lý
            .str.replace(r"[\t\r\n]", "", regex=True)  # bỏ tab, xuống dòng
            .str.strip()  # trim khoảng trắng đầu/cuối
        )
    return df


def preprocess_order(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    # 1. Clean text cho các cột thời gian và Order ID
    cols_to_clean = [
        "Created Time",
        "Order ID",
        "Paid Time",
        "RTS Time",
        "Shipped Time",
        "Delivered Time",
        "Cancelled Time",
    ]
    df = clean_columns(df, cols_to_clean)

    # 2. Chuẩn hóa Province (bỏ tiền tố "Tỉnh", "Thành phố")
    df["Province"] = (
        df["Province"]
        .str.replace(r"^(Tỉnh |Tinh )", "", regex=True)
        .str.replace(r"^(Thanh pho |Thành phố |Thành Phố )", "", regex=True)
        .str.strip()
    )

    # 3. Chuẩn hóa Country
    vietnam_aliases = [
        "Viêt Nam",
        "Vietnam",
        "The Socialist Republic of Viet Nam",
        "Socialist Republic of Vietnam",
    ]
    df["Country"] = df["Country"].replace(vietnam_aliases, "Việt Nam")

    # 4. Mapping tên tỉnh/thành
    province_mapping = {
        "Ba Ria– Vung Tau": "Bà Rịa - Vũng Tàu",
        "Bà Rịa-Vũng Tàu": "Bà Rịa - Vũng Tàu",
        "Ba Ria - Vung Tau": "Bà Rịa - Vũng Tàu",
        "Bac Giang": "Bắc Giang",
        "Bac Lieu": "Bạc Liêu",
        "Bac Ninh": "Bắc Ninh",
        "Ben Tre": "Bến Tre",
        "Binh Dinh": "Bình Định",
        "Binh Duong": "Bình Dương",
        "Binh Duong Province": "Bình Dương",
        "Binh Phuoc": "Bình Phước",
        "Binh Thuan": "Bình Thuận",
        "Ca Mau": "Cà Mau",
        "Ca Mau Province": "Cà Mau",
        "Can Tho": "Cần Thơ",
        "Phố Cần Thơ": "Cần Thơ",
        "Da Nang": "Đà Nẵng",
        "Da Nang City": "Đà Nẵng",
        "Phố Đà Nẵng": "Đà Nẵng",
        "Dak Lak": "Đắk Lắk",
        "Đắc Lắk": "Đắk Lắk",
        "Ðắk Nông": "Đắk Nông",
        "Đắk Nông": "Đắk Nông",
        "Dak Nong": "Đắk Nông",
        "Dong Nai": "Đồng Nai",
        "Dong Nai Province": "Đồng Nai",
        "Dong Thap": "Đồng Tháp",
        "Dong Thap Province": "Đồng Tháp",
        "Ha Nam": "Hà Nam",
        "Ha Noi": "Hà Nội",
        "Ha Noi City": "Hà Nội",
        "Phố Hà Nội": "Hà Nội",
        "Hai Phong": "Hải Phòng",
        "Phố Hải Phòng": "Hải Phòng",
        "Ha Tinh": "Hà Tĩnh",
        "Hau Giang": "Hậu Giang",
        "Hô-Chi-Minh-Ville": "Hồ Chí Minh",
        "Ho Chi Minh": "Hồ Chí Minh",
        "Ho Chi Minh City": "Hồ Chí Minh",
        "Kota Ho Chi Minh": "Hồ Chí Minh",
        "Hoa Binh": "Hòa Bình",
        "Hoà Bình": "Hòa Bình",
        "Hung Yen": "Hưng Yên",
        "Khanh Hoa": "Khánh Hòa",
        "Khanh Hoa Province": "Khánh Hòa",
        "Khánh Hoà": "Khánh Hòa",
        "Kien Giang": "Kiên Giang",
        "Kiến Giang": "Kiên Giang",
        "Long An Province": "Long An",
        "Nam Dinh": "Nam Định",
        "Nghe An": "Nghệ An",
        "Ninh Binh": "Ninh Bình",
        "Ninh Thuan": "Ninh Thuận",
        "Quang Binh": "Quảng Bình",
        "Quang Tri": "Quảng Trị",
        "Quang Nam": "Quảng Nam",
        "Quang Ngai": "Quảng Ngãi",
        "Quang Ninh": "Quảng Ninh",
        "Quang Ninh Province": "Quảng Ninh",
        "Soc Trang": "Sóc Trăng",
        "Tay Ninh": "Tây Ninh",
        "Thai Binh": "Thái Bình",
        "Thanh Hoa": "Thanh Hóa",
        "Thanh Hoá": "Thanh Hóa",
        "Hai Duong": "Hải Dương",
        "Thừa Thiên Huế": "Thừa Thiên-Huế",
        "Thua Thien Hue": "Thừa Thiên-Huế",
        "Vinh Long": "Vĩnh Long",
        "Tra Vinh": "Trà Vinh",
        "Vinh Phuc": "Vĩnh Phúc",
        "Cao Bang": "Cao Bằng",
        "Lai Chau": "Lai Châu",
        "Ha Giang": "Hà Giang",
        "Lam Dong": "Lâm Đồng",
        "Lao Cai": "Lào Cai",
        "Phu Tho": "Phu Tho",
        "Phu Yen": "Phú Yên",
        "Thai Nguyen": "Thái Nguyên",
        "Son La": "Sơn La",
        "Tuyen Quang": "Tuyên Quang",
        "Yen Bai": "Yên Bái",
        "Dien Bien": "Điện Biên",
        "Tien Giang": "Tiền Giang",
    }
    df["Province"] = df["Province"].replace(province_mapping)

    # 5. Chuẩn hóa SKU Category
    df["SKU Category"] = df["Seller SKU"]

    replacements = {
        r"^(COMBO-SC-ANHDUC|COMBO-SC-NGOCTRINH|COMBO-SC-MIX|SC_COMBO_MIX|SC_COMBO_MIX_LIVESTREAM|COMBO-SC_LIVESTREAM|SC_COMBO_MIX_01)$": "COMBO-SC",
        r"^SC_X1$": "SC-450g",
        r"^SC_X2$": "SC-x2-450g",
        r"^(SC_COMBO_X1|COMBO-CAYVUA-X1|SC_COMBO_X1_LIVESTREAM|COMBO-SCX1|COMBO-SCX1_LIVESTREAM)$": "COMBO-SCX1",
        r"^(SC_COMBO_X2|COMBO-SIEUCAY-X2|SC_COMBO_X2_LIVESTREAM|COMBO-SCX2|COMBO-SCX2_LIVESTREAM)$": "COMBO-SCX2",
        r"^(BTHP-Cay-200gr|BTHP_Cay)$": "BTHP-CAY",
        r"^(BTHP-200gr|BTHP_KhongCay)$": "BTHP-0CAY",
        r"^(BTHP_COMBO_MIX|BTHP003_combo_mix)$": "BTHP-COMBO",
        r"^(BTHP_COMBO_KhongCay|BTHP003_combo_kocay)$": "BTHP-COMBO-0CAY",
        r"^(BTHP_COMBO_Cay|BTHP003_combo_cay)$": "BTHP-COMBO-CAY",
        r"^BTHP-COMBO\+SC_X1$": "COMBO_BTHP_SCx1",
        r"^BTHP-COMBO\+SC_X2$": "COMBO_BTHP_SCx2",
        r"^BTHP_COMBO_MIX\+SC_X1$": "COMBO_BTHP_SCx1",
        r"^BTHP_COMBO_MIX\+SC_X2$": "COMBO_BTHP_SCx2",
        r"^(BTHP-2Cay-2KhongCay)$": "COMBO_4BTHP",
        r"^(BTHP-4Hu-KhongCay)$": "4BTHP_0CAY",
        r"^(BTHP-4Hu-Cay)$": "4BTHP_CAY",
    }

    for pattern, replacement in replacements.items():
        df["SKU Category"] = df["SKU Category"].str.replace(
            pattern, replacement, regex=True
        )

    df["Created_Timestamp"] = pd.to_datetime(
        df["Created Time"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    df["Order_Month"] = df["Created_Timestamp"].dt.strftime("%Y-%m")

    return df


def preprocess_income(df_income: pd.DataFrame) -> pd.DataFrame:

    df_income.columns = df_income.columns.str.strip()
    df_income["ABS_Total_Fees"] = df_income["Total fees"].abs()
    df_income["Classify"] = (
        df_income["Related order ID"]
        .duplicated(keep=False)
        .map({True: "Duplicate", False: "Not Duplicate"})
    )
    df_income["Paydouble"] = df_income.duplicated(
        subset=["Related order ID", "Order/adjustment ID"], keep=False
    ).map({True: "Yes", False: "No"})
    df_income["Order/adjustment ID"] = df_income["Order/adjustment ID"].astype(
        str)
    df_income["Related order ID"] = df_income["Related order ID"].astype(str)
    df_income["OID_start7"] = (
        df_income["Order/adjustment ID"].astype(str).str.startswith("7")
    )
    df_income["Not_Order_Type"] = df_income["Type"].astype(str) != "Order"
    df_income["RID_count"] = df_income.groupby("Related order ID")[
        "Related order ID"
    ].transform("count")
    grouped = df_income.groupby("Related order ID")
    is_compensation = grouped["OID_start7"].transform("any") | grouped[
        "Not_Order_Type"
    ].transform("any")
    is_doublepaid = (df_income["RID_count"] > 1) & ~is_compensation
    df_income["Actually Order Type"] = "Normal"
    df_income.loc[is_compensation, "Actually Order Type"] = "Compensation"
    df_income.loc[is_doublepaid, "Actually Order Type"] = "DoublePaid"
    df_income.drop(
        columns=["OID_start7", "Not_Order_Type", "RID_count"], inplace=True)
    df_income["Order settled time"] = pd.to_datetime(
        df_income["Order settled time"], format="%Y/%m/%d", errors="coerce"
    ).dt.date

    return df_income


# Lấy service account từ secrets
try:
    service_account_raw = st.secrets["google"]["service_account"]
    service_account_info = json.loads(service_account_raw)
except Exception as e:
    st.error(f"❌ Không tìm thấy hoặc lỗi JSON trong secrets: {e}")
    st.stop()

# Tạo credentials
SCOPES = ["https://www.googleapis.com/auth/drive"]
credentials = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

# Kết nối Google Drive
drive_service = build("drive", "v3", credentials=credentials)
st.success("✅ Kết nối Google Drive thành công!")

FOLDER_ID = "114wWkA09hPYc5cHoX-sENZEYiOCu6Ae2"

# ========================
# 3️⃣ Hàm tải file từ Drive
# ========================


def download_parquet_from_drive(filename, folder_id=FOLDER_ID):
    """Tìm file theo tên trong folder Drive, tải về và đọc bằng pandas."""
    query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])

    if not items:
        return None

    file_id = items[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_parquet(fh)


def download_json_from_drive(filename, folder_id=FOLDER_ID):
    """Tìm file theo tên trong folder Drive, tải về và đọc JSON."""
    query = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])

    if not items:
        return None

    file_id = items[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    content = fh.read()  # bytes
    return json.loads(content.decode("utf-8"))


# ========================
# 4️⃣ Giao diện Streamlit
# ========================
st.markdown(
    """
    <div style='top: 60px; left: 40px; z-index: 1000;'>
        <img src='https://raw.githubusercontent.com/CaptainCattt/Report_of_shopee/main/logo-lamvlog.png' width='150'/>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <h2 style='text-align: center;
               background: linear-gradient(90deg, #FE2C55 0%, #25F4EE 100%);
               -webkit-background-clip: text;
               -webkit-text-fill-color: transparent;
               font-size: 48px;
               font-weight: bold;
               margin-bottom: 25px;
               font-family: Arial, Helvetica, sans-serif;'>
        Dashboard TikTok BI
    </h2>
    """,
    unsafe_allow_html=True,
)

# Session login
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = ""

# Hàm check login


def check_login(input_user, input_pass):
    # giả sử đã có download_json_from_drive
    users = download_json_from_drive("User.json")
    if not users:
        st.error("⚠️ Không tìm thấy file User.json trên Drive")
        return False

    for u in users:
        if input_user.strip() == u.get("username", "").strip() and input_pass.strip() == u.get("password", "").strip():
            st.session_state.logged_in = True
            st.session_state.username = input_user
            return True

    st.error("❌ Sai username hoặc password")
    return False


# Form login
if not st.session_state.logged_in:
    with st.form("login_form"):
        st.subheader("Đăng nhập để vào App")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")
    if submit:
        check_login(username, password)

if st.session_state.logged_in:
    st.success(f"👋 Chào mừng {st.session_state.username}!")

    # Khởi tạo session_state mặc định
    for key, val in {
        "auto_load_done": False,
        "df_order_drive": None,
        "df_income_drive": None,
        "is_loading": False,
        "load_refresh_type": "load"
    }.items():
        if key not in st.session_state:
            st.session_state[key] = val

    # =========================
    # Nút Load / Refresh
    # =========================
    if not st.session_state.is_loading and not st.session_state.auto_load_done:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔎 Load data", key="btn_load", use_container_width=True):
                st.session_state.load_refresh_type = "load"
                st.session_state.is_loading = True
        with col2:
            if st.button("🔄 Refresh data", key="btn_refresh", use_container_width=True):
                st.session_state.load_refresh_type = "refresh"
                st.session_state.is_loading = True

    # =========================
    # Spinner khi đang load
    # =========================
    if st.session_state.is_loading:
        action_text = "Refresh" if st.session_state.load_refresh_type == "refresh" else "Load"
        with st.spinner(f"⏳ {action_text} dữ liệu từ Google Drive..."):
            try:
                refresh = st.session_state.load_refresh_type == "refresh"
                order_df = download_parquet_from_drive("ALL_data_tiktok.parquet") \
                    if refresh or st.session_state.df_order_drive is None else st.session_state.df_order_drive
                income_df = download_parquet_from_drive("INCOME_all_data_tiktok.parquet") \
                    if refresh or st.session_state.df_income_drive is None else st.session_state.df_income_drive

                if order_df is not None and income_df is not None:
                    st.session_state.df_order_drive = order_df
                    st.session_state.df_income_drive = income_df
                    st.session_state.df_order = preprocess_order(order_df)
                    st.session_state.df_income = preprocess_income(income_df)

                    con = duckdb.connect(database=":memory:")
                    con.register("orders", st.session_state.df_order)
                    con.register("income", st.session_state.df_income)

                    st.success(
                        f"✅ {action_text} dữ liệu thành công!\n\n"
                        f"📦 Orders: {len(st.session_state.df_order):,}\n"
                        f"💰 Income: {len(st.session_state.df_income):,}\n"
                        f"🕒 Cập nhật: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                else:
                    st.warning("⚠️ Không tìm thấy dữ liệu từ Drive!")
            except Exception as e:
                st.error(f"❌ Lỗi khi load dữ liệu: {e}")
            finally:
                st.session_state.is_loading = False
                st.session_state.auto_load_done = True

    # =========================
    # Thông báo nếu đã load
    # =========================
    if st.session_state.auto_load_done and not st.session_state.is_loading:
        st.info("✅ Dữ liệu đã được load.")

    # =========================
    # Lấy dữ liệu để sử dụng
    # =========================
    order_df = st.session_state.df_order_drive
    income_df = st.session_state.df_income_drive

    # =========================

    if "df_order" in st.session_state and "df_income" in st.session_state:
        order = st.session_state.df_order
        income = st.session_state.df_income
        # df_joined = st.session_state.df_joined

        con = duckdb.connect(database=":memory:")
        con.register("orders", order)
        con.register("income", income)

        # Preview join chỉ lấy 10 bản ghi thôi
        df_preview = con.execute(
            """
            SELECT  o.*, i.*
            FROM orders o
            INNER JOIN income i
            ON o."Order ID" = i."Related order ID"
            ORDER BY o."Created Time" ASC
            LIMIT 10

        """
        ).fetchdf()

        st.session_state.df_preview = df_preview

        df_orders_by_month = con.execute(
            """
            SELECT
                strftime("Created_Timestamp", '%Y-%m') AS Order_Month,
                COUNT(DISTINCT "Order ID") AS "Số đơn hàng",
                COUNT(DISTINCT CASE WHEN "Order Status" = 'Canceled' THEN "Order ID" END) AS "Số đơn huỷ",
                COUNT(DISTINCT CASE WHEN "Order Status" = 'Completed' THEN "Order ID" END) AS "Số đơn thành công"
            FROM orders
            GROUP BY strftime("Created_Timestamp", '%Y-%m')
            ORDER BY Order_Month
            """
        ).fetchdf()

        df_orders_by_month["Order_Month"] = pd.Categorical(
            df_orders_by_month["Order_Month"],
            categories=sorted(df_orders_by_month["Order_Month"].unique()),
            ordered=True,
        )

        st.session_state.df_orders_by_month = df_orders_by_month

        fig_orders_by_month = px.bar(
            df_orders_by_month,
            x="Order_Month",
            y=["Số đơn hàng", "Số đơn thành công", "Số đơn huỷ"],
            barmode="group",
            title="📊 Thống kê đơn hàng theo tháng",
            labels={
                "value": "Số lượng",
                "Order_Month": "Tháng",
                "variable": "Loại đơn hàng",
            },
            color_discrete_map={
                "Số đơn hàng": "blue",
                "Số đơn thành công": "darkgreen",
                "Số đơn huỷ": "red",
            },
        )
        fig_orders_by_month.update_xaxes(type="category")
        st.session_state.fig_orders_by_month = fig_orders_by_month

        sku_summary = con.execute(
            """
            SELECT
                "SKU Category",
                SUM(Quantity) AS "Tổng số lượng sản phẩm",
                SUM(CASE WHEN "Order Status" = 'Completed' THEN 1 ELSE 0 END) AS "Sản phẩm của đơn hoàn thành"
            FROM orders
            WHERE "SKU Category" IS NOT NULL
            GROUP BY "SKU Category"
            ORDER BY "Tổng số lượng sản phẩm" DESC
            """
        ).fetchdf()

        st.session_state.sku_summary = sku_summary

        fig_sku_summary = px.bar(
            sku_summary,
            x="SKU Category",
            y="Tổng số lượng sản phẩm",
            text="Tổng số lượng sản phẩm",
            title="📊 Tổng số lượng bán ra theo SKU 📊",
            color="SKU Category",
            height=600,
            width=800,
        )
        fig_sku_summary.update_traces(
            texttemplate="%{text:.2s}", textposition="outside")
        fig_sku_summary.update_layout(xaxis_tickangle=-45)

        st.session_state.fig_sku_summary = fig_sku_summary

        sku_monthly = con.execute(
            """
            SELECT
                strftime("Created_Timestamp", '%Y-%m') AS "Tháng",
                "SKU Category" AS "Loại sản phẩm",
                SUM(Quantity) AS "Total_Quantity",
                SUM(CASE WHEN "Order Status" = 'Completed' THEN 1 ELSE 0 END) AS "Completed_Orders"
            FROM orders
            WHERE "SKU Category" IS NOT NULL
            GROUP BY 1,2
            ORDER BY "Tháng", "Total_Quantity" DESC
            """
        ).fetchdf()

        st.session_state.sku_monthly = sku_monthly

        fig_sku_monthly = px.bar(
            sku_monthly,
            x="Tháng",
            y="Total_Quantity",
            color="Loại sản phẩm",
            barmode="group",
            text="Total_Quantity",
            title="📊 Số lượng bán ra theo SKU Category từng tháng 📊",
            height=600,
            width=1200,
        )
        fig_sku_monthly.update_traces(
            textposition="outside", texttemplate="%{text:,}")
        fig_sku_monthly.update_layout(
            xaxis_title="Tháng",
            yaxis_title="Số lượng bán ra",
            legend_title="Loại sản phẩm",
            xaxis=dict(tickangle=-45, showgrid=True, type="category"),
            yaxis=dict(showgrid=True),
            font=dict(size=14),
        )

        st.session_state.fig_sku_monthly = fig_sku_monthly

        fig_pie_sku_summary = px.pie(
            sku_summary,
            names="SKU Category",
            values="Tổng số lượng sản phẩm",
            title="📦 Tỷ trọng số lượng sản phẩm theo SKU",
            hole=0.3,
        )

        fig_pie_sku_summary.update_traces(
            textinfo="percent+value",
            textfont_size=12,
            pull=[0.05] * len(sku_summary),
        )

        st.session_state.fig_pie_sku_summary = fig_pie_sku_summary

        income_by_month = con.execute(
            """
            SELECT
                strftime("Order settled time", '%Y-%m') AS Month,
                SUM(CAST("Total revenue" AS DOUBLE)) AS "Doanh thu",
                SUM(CAST("Total settlement amount" AS DOUBLE)) AS "Doanh thu thực nhận"
            FROM income
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchdf()

        # 👉 Thêm cột format hiển thị đẹp
        income_by_month["Doanh thu_fmt"] = income_by_month["Doanh thu"].apply(
            lambda x: f"{x:,.0f} ₫"
        )
        income_by_month["Doanh thu thực nhận_fmt"] = income_by_month[
            "Doanh thu thực nhận"
        ].apply(lambda x: f"{x:,.0f} ₫")

        st.session_state.income_by_month_pd = income_by_month[
            ["Month", "Doanh thu_fmt", "Doanh thu thực nhận_fmt"]
        ]

        fig_income_by_month = px.bar(
            income_by_month,
            x="Month",
            y=["Doanh thu", "Doanh thu thực nhận"],
            barmode="group",
            title="📊 Thống kê doanh thu theo tháng",
            labels={
                "value": "Total revenue",
                "Month": "Tháng",
            },
            color_discrete_map={
                "Doanh thu thực nhận": "blue",
                "Doanh thu": "darkgreen",
            },
        )

        fig_income_by_month.update_yaxes(
            tickformat=",.0f", title="Doanh thu (₫)")
        fig_income_by_month.update_xaxes(type="category")

        st.session_state.fig_income_by_month = fig_income_by_month

        # --- Nút xuất Top 10 người mua ---
        with st.sidebar:
            st.write("### 🏆 Top 10 người mua nhiều nhất 🏆")

            if st.button("Xem Top 10 người mua"):
                query_top10_buyer = """
                    SELECT "Buyer Username", COUNT("Order ID") AS "Total orders"
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Order Status" = 'Completed' AND "Buyer Username" IS NOT NULL
                    GROUP BY "Buyer Username"
                    ORDER BY "Total orders" DESC
                    LIMIT 10
                """
                df_top10_buyers = con.execute(query_top10_buyer).fetchdf()
                st.session_state.df_top10_buyers = df_top10_buyers

                if "df_top10_buyers" in st.session_state:
                    st.dataframe(st.session_state.df_top10_buyers)

        # --- Nút xuất Top 10 tỉnh thành ---
        with st.sidebar:
            st.write("### 🏢 Top 10 tỉnh thành mua nhiều nhất 🏢")

            if st.button("Xem Top 10 tỉnh thành"):
                query_top10_province = """
                    SELECT "Province", COUNT("Order ID") AS "Total orders"
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Order Status" = 'Completed' AND "Province" IS NOT NULL
                    GROUP BY "Province"
                    ORDER BY "Total orders" DESC
                    LIMIT 10
                """
                df_top10_province = con.execute(query_top10_province).fetchdf()
                st.session_state.df_top10_province = df_top10_province
                if "df_top10_province" in st.session_state:
                    st.dataframe(st.session_state.df_top10_province)

        # --- Nút xuất danh sách dơn hàng Điều chỉnh ---
        with st.sidebar:
            st.write("### ‼️ Danh sách đơn hàng Điều chỉnh ‼️")

            if st.button("Xem danh sách"):
                query_ = """
                    SELECT "Order ID", "Type", "Order Status", "SKU Category", "Quantity", "Total revenue", "Total settlement amount", "Created Time"
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Actually Order Type" = 'Compensation' AND "Type" != 'Order'
                    ORDER BY "Created_Timestamp"
                    DESC
                    """
                df_ = con.execute(query_).fetchdf()
                st.session_state.df_ = df_
                if "df_" in st.session_state:
                    st.dataframe(st.session_state.df_)

            # --- Time-series Analysis ---
        with st.sidebar:
            st.write("### 🌍 Phân tích khu vực 🌍")

            if st.button("Xem kết quả"):
                query_1 = """
                    SELECT o."Province",
                        COUNT(DISTINCT o."Order ID") AS "Số đơn hàng",
                        SUM(i."Total revenue") AS "Doanh thu",
                        AVG(CASE WHEN o."Cancelation/Return Type" = 'Return/Refund' THEN 1 ELSE 0 END) AS "Tỷ lệ hoàng hoàn"
                    FROM orders o
                    JOIN income i ON o."Order ID" = i."Related order ID"
                    GROUP BY o."Province"
                    ORDER BY "Doanh thu" DESC;
                    """
                df_1 = con.execute(query_1).fetchdf()
                df_1["Doanh thu"] = df_1["Doanh thu"].apply(
                    lambda x: f"{x:,.0f} ₫")
                st.session_state.df_1 = df_1
                if "df_1" in st.session_state:
                    st.dataframe(st.session_state.df_1)

        with st.sidebar:
            st.write("### 📦 Phân tích sản phẩm 📦")

            if st.button("Xem phân tích"):
                query_2 = """
                    SELECT o."Product Name",
                            o."Seller SKU",
                            o."SKU Category",

                        COUNT(DISTINCT o."Order ID") AS "Số đơn hàng",
                        SUM(i."Total revenue") AS "Doanh thu",
                        SUM(CASE WHEN o."Cancelation/Return Type" = 'Return/Refund' THEN 1 ELSE 0 END) AS "Số đơn hoàn trả",
                    FROM orders o
                    JOIN income i ON o."Order ID" = i."Related order ID"
                    GROUP BY o."Product Name", o."Seller SKU", o."SKU Category"
                    ORDER BY "Doanh thu" DESC

                    """
                df_2 = con.execute(query_2).fetchdf()
                df_2["Doanh thu"] = df_2["Doanh thu"].apply(
                    lambda x: f"{x:,.0f} ₫")
                st.session_state.df_2 = df_2
                if "df_2" in st.session_state:
                    st.dataframe(st.session_state.df_2)

        # --- Form tìm kiếm Order ID ---
        with st.sidebar.form("search_order_form"):
            st.write("### 🔍 Tìm kiếm Order ID")
            order_id = st.text_input("Nhập Order ID:", key="search_order_id")
            submit_btn = st.form_submit_button("Tìm kiếm")

            if submit_btn and order_id:
                query = f"""
                    SELECT *
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Order ID" = '{order_id}'
                """
                df_filtered = con.execute(query).fetchdf()
                st.session_state.df_search_result = df_filtered

            if "df_search_result" in st.session_state:
                df_filtered = st.session_state.df_search_result
                if not df_filtered.empty:
                    st.success(
                        f"Đã tìm thấy {len(df_filtered)} bản ghi cho Order ID {order_id}"
                    )
                    st.dataframe(df_filtered)
                else:
                    st.warning(f"Không tìm thấy Order ID {order_id}")

        # --- Form tìm kiếm theo Tỉnh/Thành ---
        with st.sidebar.form("search_city_form"):
            st.write("### 🏙️ Tìm kiếm theo Tỉnh/Thành")
            city = st.text_input("Nhập Tỉnh/Thành:", key="search_city")
            submit_city_btn = st.form_submit_button("Tìm kiếm")

            if submit_city_btn and city:
                query = f"""
                    SELECT "Order ID", "Order Status", "SKU Category", "Quantity", "Total revenue", "Total settlement amount", "Created Time", "Province", "Buyer Username"
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Order Status" = 'Completed' AND Province ILIKE '%{city}%'
                """
                df_filtered_city = con.execute(query).fetchdf()
                st.session_state.df_city_result = df_filtered_city

            if "df_city_result" in st.session_state:
                df_filtered_city = st.session_state.df_city_result
                if not df_filtered_city.empty:
                    st.success(
                        f"Đã tìm thấy {len(df_filtered_city)} bản ghi tại {city}")
                    st.dataframe(df_filtered_city)
                else:
                    st.warning(f"Không tìm thấy đơn hàng nào tại {city}")

        # --- Form nhập câu lệnh SQL ---
        with st.form("sql_query_form"):
            sql_query = st.text_area("Nhập câu lệnh SQL:",
                                     height=200, key="sql_input")
            run_query = st.form_submit_button("Chạy query")

        if run_query and sql_query.strip():
            try:
                df_query = con.execute(sql_query).fetchdf()
                st.success(
                    f"✅ Query chạy thành công! Trả về {len(df_query)} dòng.")
                st.dataframe(df_query)

                csv = df_query.to_csv(index=False).encode("utf-8")

                st.download_button(
                    label="📥 Tải kết quả CSV",
                    data=csv,
                    file_name="query_result.csv",
                    mime="text/csv",
                )

            except Exception as e:
                st.error(f"❌ Lỗi khi chạy query: {e}")

        # --- Form tìm kiếm SKU ID ---
        with st.sidebar.form("search_sku_id_form"):
            st.write("### 🔍 Tìm kiếm SKU ID")
            sku_id = st.text_input("Nhập SKU ID:", key="search_sku_id")
            submit_sku_btn = st.form_submit_button("Tìm kiếm")

            if submit_sku_btn and sku_id:
                query = f"""
                    SELECT "Order ID", "Order Status", "SKU Category", "Quantity", "Total revenue", "Total settlement amount", "Province", "Buyer Username"
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "SKU ID" = '{sku_id}'
                """
                df_filtered_sku = con.execute(query).fetchdf()
                st.session_state.df_search_result_sku = df_filtered_sku

            if "df_search_result_sku" in st.session_state:
                df_filtered_sku = st.session_state.df_search_result_sku
                if not df_filtered_sku.empty:
                    st.success(
                        f"Đã tìm thấy {len(df_filtered_sku)} bản ghi cho SKU ID {sku_id}"
                    )
                    st.dataframe(df_filtered_sku)
                else:
                    st.warning(f"Không tìm thấy SKU ID {sku_id}")

        # --- Form tìm kiếm OrderID từ tên khách hàng ---
        with st.sidebar.form("search_order_id_form_buyer"):
            st.write("### 🔍 Tìm kiếm Order ID từ tên Người mua")
            name_buyer = st.text_input(
                "Nhập tên của người mua:", key="search_name_buyer")
            submit_name_buyer = st.form_submit_button("Tìm kiếm")

            if submit_name_buyer and name_buyer:
                query = f"""
                    SELECT *
                    FROM orders o
                    INNER JOIN income i
                    ON o."Order ID" = i."Related order ID"
                    WHERE "Buyer Username" = '{name_buyer}'
                """
                df_filtered_buyer = con.execute(query).fetchdf()
                df_filtered_buyer_1 = df_filtered_buyer[
                    [
                        "Order ID",
                        "Order Status",
                        "SKU Category",
                        "Quantity",
                        "Total revenue",
                        "Total settlement amount",
                        "Created Time",
                        "Province",
                    ]
                ]
                st.session_state.df_filtered_buyer_1 = df_filtered_buyer_1

            if "df_filtered_buyer_1" in st.session_state:
                df_filtered_buyer = st.session_state.df_filtered_buyer_1
                if not df_filtered_buyer.empty:
                    st.success(
                        f"Đã tìm thấy {len(df_filtered_buyer)} bản ghi Order ID cho người mua '{name_buyer}'"
                    )
                    st.dataframe(df_filtered_buyer)
                else:
                    st.warning(
                        f"Không tìm thấy Order ID của người mua '{name_buyer}'")

    # Hiển thị các kết quả tìm kiếm
    if "df_preview" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px; '>📅 Thông tin một số đơn hàng gần nhất 📅</h2>",
            unsafe_allow_html=True,
        )

        st.dataframe(st.session_state.df_preview)

    # =========================
    st.markdown("<br><br><br>", unsafe_allow_html=True)

    # Đon hàng theo tháng
    if "df_orders_by_month" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px;'>📅 Thống kê đơn hàng theo tháng 📅</h2>",
            unsafe_allow_html=True,
        )

        st.dataframe(st.session_state.df_orders_by_month)

    # Đồ thị đơn hàng theo tháng
    if "fig_orders_by_month" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px;'>📊 Thống kê đơn hàng theo tháng 📊</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(st.session_state.fig_orders_by_month,
                        use_container_width=True)

    # Doanh thu theo tháng
    if "income_by_month_pd" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px;'>💰 Doanh thu theo tháng 💰</h2>",
            unsafe_allow_html=True,
        )
        st.dataframe(st.session_state.income_by_month_pd)

    # Doanh thu theo tháng - đồ thị
    if "fig_income_by_month" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px; '>📊 Doanh thu theo tháng 📊</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(st.session_state.fig_income_by_month,
                        use_container_width=True)

    # Tổng số lượng bán ra theo SKU
    if "sku_summary" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px; '>📦 Tổng số lượng bán ra theo SKU 📦</h2>",
            unsafe_allow_html=True,
        )
        st.dataframe(st.session_state.sku_summary)

    # Đồ thị tổng số lượng bán ra theo SKU
    if "fig_sku_summary" in st.session_state:
        st.plotly_chart(st.session_state.fig_sku_summary,
                        use_container_width=True)

    # Biểu đồ tròn tỷ trọng số lượng sản phẩm theo SKU
    if "fig_pie_sku_summary" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 28px; '>📊 Tỷ trọng số lượng sản phẩm theo SKU 📊</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(st.session_state.fig_pie_sku_summary,
                        use_container_width=True)

    # Tổng số lượng bán ra theo SKU Category mỗi tháng
    if "sku_monthly" in st.session_state:
        st.markdown(
            "<h2 style='text-align: center; font-size: 26px; '>📦 Tổng số lượng bán ra theo SKU Category mỗi tháng 📦</h2>",
            unsafe_allow_html=True,
        )
        st.dataframe(st.session_state.sku_monthly)

    # Đồ thị tổng số lượng bán ra theo SKU Category mỗi tháng
    if "fig_sku_monthly" in st.session_state:
        st.plotly_chart(st.session_state.fig_sku_monthly,
                        use_container_width=True)
