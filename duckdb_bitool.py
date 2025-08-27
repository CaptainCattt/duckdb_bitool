import streamlit as st
import pandas as pd
import plotly.express as px
from itertools import chain
import duckdb

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
    df_income["Order/adjustment ID"] = df_income["Order/adjustment ID"].astype(str)
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
    df_income.drop(columns=["OID_start7", "Not_Order_Type", "RID_count"], inplace=True)
    df_income["Order settled time"] = pd.to_datetime(
        df_income["Order settled time"], format="%Y/%m/%d", errors="coerce"
    ).dt.date

    return df_income


# ========================
# Giao diện Streamlit
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

# Upload file parquet
st.markdown(
    "<h3>📥 Upload file TikTok Order</h3>",
    unsafe_allow_html=True,
)
uploaded_file_order = st.file_uploader(
    "Upload file TikTok Order (.parquet)", type=["parquet"]
)

# Upload file parquet
st.markdown(
    "<h3>📥 Upload file TikTok Income</h3>",
    unsafe_allow_html=True,
)
uploaded_file_income = st.file_uploader(
    "Upload file TikTok Income (.parquet)", type=["parquet"]
)

# Chia layout thành 2 cột, nút sẽ nằm 2 đầu
col1, col_space, col2 = st.columns([1, 2.8, 1])

with col1:
    load_btn = st.button("🔎 Load data")

with col2:
    refresh_btn = st.button("🔄 Refresh data")

if load_btn:
    if uploaded_file_order and uploaded_file_income:
        # Load parquet vào pandas
        df_order = pd.read_parquet(uploaded_file_order)
        df_income = pd.read_parquet(uploaded_file_income)

        # Preprocess
        df_order = preprocess_order(df_order)
        df_income = preprocess_income(df_income)

        # # Join bằng DuckDB

        con = duckdb.connect(database=":memory:")
        con.register("orders", df_order)
        con.register("income", df_income)
        df_joined = duckdb.query(
            """
            SELECT o.*, i.*
            FROM df_order o
            INNER JOIN df_income i
                ON o."Order ID" = i."Related order ID"
        """
        ).fetchdf()
        df_preview = df_joined.head(10)

        # Lưu session state
        st.session_state.df_order = df_order
        st.session_state.df_income = df_income

        # st.session_state.df_joined = df_joined
        # st.session_state.df_preview = df_preview
        # st.success(
        #     f"✅ Đã load dữ liệu: Orders {len(df_order):,}, Income {len(df_income):,}"
        # )
        st.success(f"✅ Đã load và join xong, tổng số bản ghi: {len(df_joined):,}")
    else:
        st.warning("⚠️ Vui lòng upload đủ cả 2 file trước khi load!")

if refresh_btn:
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

if "df_order" in st.session_state and "df_income" in st.session_state:
    order = st.session_state.df_order
    income = st.session_state.df_income
    # df_joined = st.session_state.df_joined

    con = duckdb.connect(database=":memory:")
    con.register("orders", order)
    con.register("income", income)

    # Preview join chỉ lấy 10 bản ghi thôi
    # df_preview = con.execute(
    #     """
    #     SELECT  o.*, i.*
    #     FROM orders o
    #     INNER JOIN income i
    #     ON o."Order ID" = i."Related order ID"
    #     LIMIT 10
    # """
    # ).fetchdf()

    # st.session_state.df_preview = df_preview

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
    fig_sku_summary.update_traces(texttemplate="%{text:.2s}", textposition="outside")
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
    fig_sku_monthly.update_traces(textposition="outside", texttemplate="%{text:,}")
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

    fig_income_by_month.update_yaxes(tickformat=",.0f", title="Doanh thu (₫)")
    fig_income_by_month.update_xaxes(type="category")

    st.session_state.fig_income_by_month = fig_income_by_month

df_joined = st.session_state.get("df_joined", None)

# --- Form tìm kiếm Order ID ---
with st.sidebar.form("search_order_form"):
    st.write("### 🔍 Tìm kiếm Order ID")
    order_id = st.text_input("Nhập Order ID:", key="search_order_id")
    submit_btn = st.form_submit_button("Tìm kiếm")

    if submit_btn and order_id:
        query = f"""
            SELECT *
            FROM df_joined
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
            FROM df_joined
            WHERE Province ILIKE '%{city}%'
        """
        df_filtered_city = con.execute(query).fetchdf()
        st.session_state.df_city_result = df_filtered_city

    if "df_city_result" in st.session_state:
        df_filtered_city = st.session_state.df_city_result
        if not df_filtered_city.empty:
            st.success(f"Đã tìm thấy {len(df_filtered_city)} bản ghi tại {city}")
            st.dataframe(df_filtered_city)
        else:
            st.warning(f"Không tìm thấy đơn hàng nào tại {city}")

# --- Form tìm kiếm SKU ID ---
with st.sidebar.form("search_sku_id_form"):
    st.write("### 🔍 Tìm kiếm SKU ID")
    sku_id = st.text_input("Nhập SKU ID:", key="search_sku_id")
    submit_sku_btn = st.form_submit_button("Tìm kiếm")

    if submit_sku_btn and sku_id:
        query = f"""
            SELECT "Order ID", "Order Status", "SKU Category", "Quantity", "Total revenue", "Total settlement amount", "Province", "Buyer Username"
            FROM df_joined
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
    name_buyer = st.text_input("Nhập tên của người mua:", key="search_name_buyer")
    submit_name_buyer = st.form_submit_button("Tìm kiếm")

    if submit_name_buyer and name_buyer:
        query = f"""
            SELECT *
            FROM df_joined
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
            st.warning(f"Không tìm thấy Order ID của người mua '{name_buyer}'")

if "df_joined" in st.session_state:
    # --- Nút xuất Top 10 người mua ---
    with st.sidebar:
        st.write("### 🏆 Top 10 người mua nhiều nhất 🏆")

        if st.button("Xem Top 10 người mua"):
            query_top10_buyer = """
                SELECT "Buyer Username", COUNT("Order ID") AS "Total orders"
                FROM df_joined
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
                FROM df_joined
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
                FROM df_joined
                WHERE "Actually Order Type" = 'Compensation' AND "Type" != 'Order'
                ORDER BY "Created_Timestamp" 
                DESC
                """
            df_ = con.execute(query_).fetchdf()
            st.session_state.df_ = df_
            if "df_" in st.session_state:
                st.dataframe(st.session_state.df_)

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
    st.plotly_chart(st.session_state.fig_orders_by_month, use_container_width=True)

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
    st.plotly_chart(st.session_state.fig_income_by_month, use_container_width=True)

# Tổng số lượng bán ra theo SKU
if "sku_summary" in st.session_state:
    st.markdown(
        "<h2 style='text-align: center; font-size: 28px; '>📦 Tổng số lượng bán ra theo SKU 📦</h2>",
        unsafe_allow_html=True,
    )
    st.dataframe(st.session_state.sku_summary)

# Đồ thị tổng số lượng bán ra theo SKU
if "fig_sku_summary" in st.session_state:
    st.plotly_chart(st.session_state.fig_sku_summary, use_container_width=True)

# Biểu đồ tròn tỷ trọng số lượng sản phẩm theo SKU
if "fig_pie_sku_summary" in st.session_state:
    st.markdown(
        "<h2 style='text-align: center; font-size: 28px; '>📊 Tỷ trọng số lượng sản phẩm theo SKU 📊</h2>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(st.session_state.fig_pie_sku_summary, use_container_width=True)

# Tổng số lượng bán ra theo SKU Category mỗi tháng
if "sku_monthly" in st.session_state:
    st.markdown(
        "<h2 style='text-align: center; font-size: 26px; '>📦 Tổng số lượng bán ra theo SKU Category mỗi tháng 📦</h2>",
        unsafe_allow_html=True,
    )
    st.dataframe(st.session_state.sku_monthly)

# Đồ thị tổng số lượng bán ra theo SKU Category mỗi tháng
if "fig_sku_monthly" in st.session_state:
    st.plotly_chart(st.session_state.fig_sku_monthly, use_container_width=True)
