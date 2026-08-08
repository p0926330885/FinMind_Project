import streamlit as st
import pandas as pd
from FinMind.data import DataLoader

st.title("📈 台股歷史股價觀測站 (FinMind)")

# 畫面左邊的參數設定區
st.sidebar.header("參數設定")
token = st.sidebar.text_input("輸入 FinMind API Token", type="password")
stock_id = st.sidebar.text_input("股票代碼", value="2330")
start_date = st.sidebar.date_input("開始日期", value=pd.to_datetime("2026-01-01"))
end_date = st.sidebar.date_input("結束日期", value=pd.to_datetime("2026-08-01"))

if st.sidebar.button("開始抓取資料"):
    if not token:
        st.error("請先輸入 FinMind API Token 才能抓取資料喔！")
    else:
        with st.spinner("資料載入中..."):
            try:
                # 初始化 FinMind 並且抓取日成交資訊
                api = DataLoader()
                api.login_by_token(api_token=token)
                
                df = api.taiwan_stock_daily(
                    stock_id=stock_id,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d")
                )
                
                if df.empty:
                    st.warning("查無此期間的股價資料，請檢查代碼或日期。")
                else:
                    st.success(f"成功載入 {stock_id} 股價資料！")
                    
                    # 畫出折線圖
                    df['date'] = pd.to_datetime(df['date'])
                    chart_data = df.set_index('date')[['close']]
                    st.line_chart(chart_data)
                    
                    # 顯示資料表格
                    st.subheader("原始數據資料表")
                    st.dataframe(df)
            except Exception as e:
                st.error(f"發生錯誤: {e}")
