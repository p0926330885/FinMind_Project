# FinMind_Project

台股自訂板塊盤後法人資金儀表板。

## 線上入口

- 儀表板首頁：`https://p0926330885.github.io/FinMind_Project/`
- 直接開啟儀表板：`https://p0926330885.github.io/FinMind_Project/dashboard.html`
- 使用說明：`https://p0926330885.github.io/FinMind_Project/guide.html`

## 主要功能

- 17 個自訂板塊（清單在 sectors_config.json）
- 三大法人淨流入
- 挹注強度（淨流入 ÷ 成交值）
- 近 20 日異常度 z
- 近 20 日資金輪動播放
- 板塊內部資金分歧光暈
- 點擊板塊查看外資、投信與成分股明細

## 資料更新

GitHub Actions 於台灣時間週一至週五 18:37、20:47 各執行一次管線（FinMind 法人資料約 20:00 入庫，GitHub 排程可能延遲，請以儀表板「資料日」為準），產生 `data/bubble_data.json`。

> 本工具為盤後研究輔助，不構成投資建議。
