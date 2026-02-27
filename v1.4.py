import streamlit as st
import pandas as pd
import re
import io

st.set_page_config(page_title="Fred ETL V1.4 (结构化列示与勾稽面板)", layout="wide")

# ==========================================
# 1. 核心数据字典 (维持不变，保证提取精度)
# ==========================================
STANDARD_MAP = {
    # 资产类
    "货币资金": ["货币资金", "现金及现金等价物", "银行存款", "库存现金", "Cash and cash equivalents", "Cash at bank", "Cash and bank"],
    "交易性金融资产": ["交易性金融资产", "以公允价值计量且其变动计入当期损益的金融资产", "Trading financial assets", "Financial assets at FVTPL", r"以公允价值计量.*当期损益.*资产"],
    "衍生金融资产": ["衍生金融资产", "Derivative financial assets"],
    "应收票据": ["应收票据", "Notes receivable", "Bills receivable"],
    "应收账款": ["应收账款", "应收账款账面余额", "Accounts receivable", "A/R", "Trade receivables"],
    "坏账准备": ["坏账准备", r"[-减:：\s]*坏账准备", "Provision for bad debts"],
    "应收账款净额": ["应收账款净额", "应收账款账面价值", "Net accounts receivable"],
    "应收款项融资": ["应收款项融资", "Receivables financing"],
    "预付款项": ["预付款项", "预付账款", "Prepayments", "Advances to suppliers"],
    "其他应收款": ["其他应收款", "Other receivables"],
    "存货": ["存货", "存货余额", "Inventories", "Inventory", "Stock"],
    "在途物资": ["在途物资", "Materials in transit"],
    "原材料": ["原材料", "Raw materials"],
    "在产品": ["在产品", "Work in progress", "WIP"],
    "库存商品": ["库存商品", "完工产品", "产成品", "Finished goods"],
    "周转材料": ["周转材料", "包装物及低值易耗品", "Turnover materials"],
    "委托加工物资": ["委托加工物资", "Consigned processing materials"],
    "发出商品": ["发出商品", "Goods shipped in transit"],
    "存货跌价准备": ["存货跌价准备", r"[-减:：\s]*存货跌价准备", "Provision for decline in value of inventories"],
    "存货净额": ["存货净额", "存货账面价值", "Net inventories"],
    "合同资产": ["合同资产", "Contract assets"],
    "持有待售资产": ["持有待售资产", "Assets classified as held for sale"],
    "一年内到期的非流动资产": ["一年内到期的非流动资产", "Non-current assets due within one year"],
    "其他流动资产": ["其他流动资产", "Other current assets"],
    "流动资产合计": ["流动资产合计", "流动资产总计", r"流动资产.*[合总]", "Total current assets", "Current assets total"],
    
    "债权投资": ["债权投资", "Debt investment"],
    "其他债权投资": ["其他债权投资", "Other debt investment"],
    "长期应收款": ["长期应收款", "Long-term receivables"],
    "长期股权投资": ["长期股权投资", "Long-term equity investment", "LTI"],
    "长期股权投资减值准备": ["长期股权投资减值准备", r"[-减:：\s]*长期股权投资减值准备"],
    "长期股权投资净额": ["长期股权投资净额", "长期股权投资账面价值"],
    "其他权益工具投资": ["其他权益工具投资", "Other equity instrument investment"],
    "其他非流动金融资产": ["其他非流动金融资产", "Other non-current financial assets"],
    "投资性房地产": ["投资性房地产", "Investment properties"],
    "固定资产": ["固定资产原值", "固定资产原价", "固定资产", "Property, plant and equipment", "Fixed assets", "PPE"],
    "累计折旧": ["减:累计折旧", "减：累计折旧", "累计折旧", r"[-减:：\s]*累计折旧", "Less: Accumulated depreciation"],
    "固定资产减值准备": ["减:固定资产减值准备", "减：固定资产减值准备", "固定资产减值准备", r"[-减:：\s]*固定资产减值准备", "Less: Impairment of fixed assets"],
    "固定资产净额": ["固定资产净额", "固定资产净值", "固定资产账面价值", "Net fixed assets"],
    "在建工程": ["在建工程", "Construction in progress", "CIP"],
    "生产性生物资产": ["生产性生物资产", "Productive biological assets"],
    "油气资产": ["油气资产", "Oil and gas assets"],
    "使用权资产": ["使用权资产", "Right-of-use assets"],
    "无形资产": ["无形资产", "无形资产原价", "Intangible assets"],
    "累计摊销": ["累计摊销", r"[-减:：\s]*累计摊销", "Accumulated amortization"],
    "无形资产减值准备": ["无形资产减值准备", r"[-减:：\s]*无形资产减值准备"],
    "无形资产净额": ["无形资产净额", "无形资产账面价值", "Net intangible assets"],
    "开发支出": ["开发支出", "Development expenditure"],
    "商誉": ["商誉", "Goodwill"],
    "长期待摊费用": ["长期待摊费用", "Long-term deferred expenses", "Long-term prepaid expenses"],
    "递延所得税资产": ["递延所得税资产", "Deferred tax assets"],
    "其他非流动资产": ["其他非流动资产", "Other non-current assets"],
    "非流动资产合计": ["非流动资产合计", "非流动资产总计", r"非流动资产.*[合总]", "Total non-current assets"],
    "资产总计": ["资产总计", "资产合计", "资产总额", "Total assets"],

    # 负债类
    "短期借款": ["短期借款", "Short-term borrowings", "Short-term loans"],
    "交易性金融负债": ["交易性金融负债", "以公允价值计量且其变动计入当期损益的金融负债", "Trading financial liabilities", r"以公允价值计量.*当期损益.*负债"],
    "衍生金融负债": ["衍生金融负债", "Derivative financial liabilities"],
    "应付票据": ["应付票据", "Notes payable", "Bills payable"],
    "应付账款": ["应付账款", "Accounts payable", "A/P", "Trade payables"],
    "预收款项": ["预收款项", "预收账款", "Advances from customers"],
    "合同负债": ["合同负债", "Contract liabilities"],
    "应付职工薪酬": ["应付职工薪酬", "Employee benefits payable", "Salaries payable"],
    "应交税费": ["应交税费", "Taxes payable", "Accrued taxes"],
    "其他应付款": ["其他应付款", "Other payables"],
    "持有待售负债": ["持有待售负债", "Liabilities held for sale"],
    "一年内到期的非流动负债": ["一年内到期的非流动负债", "Non-current liabilities due within one year"],
    "其他流动负债": ["其他流动负债", "Other current liabilities"],
    "流动负债合计": ["流动负债合计", "流动负债总计", r"流动负债.*[合总]", "Total current liabilities"],
    
    "长期借款": ["长期借款", "Long-term borrowings", "Long-term loans"],
    "应付债券": ["应付债券", "Bonds payable"],
    "租赁负债": ["租赁负债", "Lease liabilities"],
    "长期应付款": ["长期应付款", "Long-term payables"],
    "预计负债": ["预计负债", "Provisions"],
    "递延收益": ["递延收益", "Deferred income"],
    "递延所得税负债": ["递延所得税负债", "Deferred tax liabilities"],
    "其他非流动负债": ["其他非流动负债", "Other non-current liabilities"],
    "非流动负债合计": ["非流动负债合计", "非流动负债总计", r"非流动负债.*[合总]", "Total non-current liabilities"],
    "负债合计": ["负债合计", "负债总额", "负债总计", r"负债.*[合总]", "Total liabilities"],

    # 所有者权益类
    "实收资本": ["实收资本", "股本", "Paid-in capital", "Share capital", r"实收资本.*股本"],
    "其他权益工具": ["其他权益工具", "Other equity instruments"],
    "优先股": ["优先股", "Preferred stock", "Preferred shares"],
    "永续债": ["永续债", "Perpetual bond"],
    "资本公积": ["资本公积", "Capital reserve"],
    "减:库存股": ["减:库存股", "库存股", "Less: Treasury shares"],
    "其他综合收益": ["其他综合收益", "Other comprehensive income", "OCI"],
    "专项储备": ["专项储备", "Special reserve"],
    "盈余公积": ["盈余公积", "Surplus reserve", "Statutory reserve"],
    "一般风险准备": ["一般风险准备", "General risk reserve"],
    "未分配利润": ["未分配利润", "Retained earnings", "Undistributed profit"],
    "归属于母公司所有者权益合计": ["归属于母公司所有者权益合计", "Equity attributable to owners of the parent"],
    "少数股东权益": ["少数股东权益", "Minority interests", "Non-controlling interests"],
    "所有者权益合计": [
        "所有者权益合计", "股东权益合计", "所有者权益总计", "股东权益总计",
        r"所有者权益.*或.*股东权益.*[合总]计", r"所有者权益.*或股东权益.*合计", 
        r"所有者权益.*[合总]", r"股东权益.*[合总]", r"所有者权益.*或.*",
        "Total equity", "Total shareholders' equity"
    ],
    "负债和所有者权益总计": [
        "负债和所有者权益总计", "负债及股东权益总计", 
        r"负债.*和.*所有者权益.*总计", r"负债.*所有者权益.*或.*股东权益.*[合总]计",
        "Total liabilities and equity"
    ]
}

COL_MAP = {
    "期末": ["期末", "本期", "本年余额", "期末数", "期末余额", "本期数", "本期金额", "本年期末", "Ending", "Closing balance"], 
    "期初": ["期初", "年初", "上年余额", "期初数", "期初余额", "年初数", "年初余额", "上年年末余额", "上期年末余额", "上年数", "上期数", "上年同期", "Opening", "Beginning balance"]
}

# ==========================================
# 2. 基础处理函数 (引擎分流，增强鲁棒性)
# ==========================================
def clean_num(text):
    if pd.isna(text): return 0.0
    t = re.sub(r'[^0-9.\-()]', '', str(text))
    if t.startswith('(') and t.endswith(')'): t = '-' + t[1:-1]
    match = re.search(r'-?\d+\.\d{1,4}|-?\d{4,}', t)
    if match:
        try:
            val = float(match.group())
            if val != 0 and val.is_integer() and 1 <= abs(val) <= 400: return None
            return round(val, 2)
        except: return 0.0
    return 0.0 if t in ['-', '0', ''] else None

def load_file(file):
    ext = file.name.split('.')[-1].lower()
    if ext == 'xlsx':
        xls = pd.read_excel(file, sheet_name=None, header=None, engine='openpyxl')
        return max(xls.values(), key=len)
    elif ext == 'xls':
        try:
            xls = pd.read_excel(file, sheet_name=None, header=None, engine='xlrd')
            return max(xls.values(), key=len)
        except ImportError:
            raise ImportError(
                "检测到旧版 Excel (.xls) 文件，但缺少 `xlrd` 依赖库。\n\n"
                "**VSCode 本地运行**：在终端输入 `pip install xlrd`\n"
                "**云端部署**：在 `requirements.txt` 中添加一行 `xlrd`"
            )
    elif ext == 'pdf':
        import pdfplumber
        with pdfplumber.open(file) as pdf:
            return pd.concat([pd.DataFrame(t) for p in pdf.pages for t in p.extract_tables() if t])
    return pd.read_csv(file, header=None)

def grid_search(df, row_key, period):
    row_aliases, col_aliases = STANDARD_MAP[row_key], COL_MAP[period]
    target_cols = []
    
    for r in range(min(20, len(df))):
        for c in range(len(df.columns)):
            cell_txt = str(df.iloc[r, c]).replace("\n", "").replace(" ", "").replace("　", "").lower()
            if not cell_txt or cell_txt == 'nan': continue
            if any((al in cell_txt) or (cell_txt in al and len(cell_txt) >= 2) for al in [a.lower() for a in col_aliases]):
                if c not in target_cols: target_cols.append(c)

    if not target_cols: return 0.0, -1, -1

    for r in range(len(df)):
        for c in range(len(df.columns)):
            raw = str(df.iloc[r,c]).replace("\n","").replace(" ","").replace("　","").lower()
            if not raw or raw == 'nan': continue
            
            is_match = any(
                re.search(a.lower(), raw) if a.startswith(r'^') or '.*' in a else a.lower() in raw 
                for a in row_aliases
            )

            if is_match:
                if "流动" in raw and "流动" not in str(row_aliases): continue
                if "非流动" in raw and "非" not in str(row_aliases): continue
                if any(ex in raw and ex not in str(row_aliases) for ex in ["其中", "减值", "准备", "跌价", "折旧", "摊销", "清理", "减:", "减：", "加:", "加：", "加项", "减项", "+"]): continue
                if (raw.startswith("-") or raw.startswith("减")) and not any(kw in str(row_aliases) for kw in ["折旧", "摊销", "准备", "坏账", "减:"]): continue
                if row_key == "其他权益工具" and "投资" in raw: continue
                
                for row_offset in [0, 1]: 
                    check_r = r + row_offset
                    if check_r >= len(df): continue
                    for tc in [tc for tc in target_cols if tc >= c]:
                        for off in [0, 1]:  
                            if tc + off < len(df.columns):
                                v = clean_num(df.iloc[check_r, tc+off])
                                if v is not None: return v, check_r, tc+off
                    for bc in range(c + 1, len(df.columns)):
                        v = clean_num(df.iloc[check_r, bc])
                        if v is not None: return v, check_r, bc
    return 0.0, -1, -1

# ==========================================
# 3. 业务逻辑计算 (V1.4 深度精简：提取公共逻辑)
# ==========================================
def calculate_net_and_totals(df_clean, p_key):
    """集中处理期初/期末的净额与合计计算"""
    if df_clean.empty: return
    col = f"{p_key}余额"
    
    # 辅助读取函数，缺失返回0
    def v(k):
        match = df_clean.loc[df_clean['标准科目'] == k, col]
        return match.values[0] if not match.empty else 0.0
        
    # 辅助写入函数，不存在则创建
    def update_or_add(k, val):
        if k in df_clean['标准科目'].values:
            df_clean.loc[df_clean['标准科目'] == k, col] = val
        else:
            df_clean.loc[len(df_clean)] = [k, val if p_key == '期初' else 0.0, val if p_key == '期末' else 0.0]

    # 1. 计算净额
    net_rules = [
        ("固定资产", ["累计折旧", "固定资产减值准备"], "固定资产净额"),
        ("无形资产", ["累计摊销", "无形资产减值准备"], "无形资产净额"),
        ("存货", ["存货跌价准备"], "存货净额"),
        ("应收账款", ["坏账准备"], "应收账款净额"),
        ("长期股权投资", ["长期股权投资减值准备"], "长期股权投资净额")
    ]
    for gross, contras, net in net_rules:
        if v(net) == 0 and v(gross) != 0:
            update_or_add(net, round(v(gross) - sum(abs(v(c)) for c in contras), 2))

    # 2. 计算各大类合计
    a_tot, l_tot, e_tot = v('资产总计'), v('负债合计'), v('所有者权益合计')
    
    if a_tot == 0 and (v('流动资产合计') != 0 or v('非流动资产合计') != 0):
        update_or_add('资产总计', round(v('流动资产合计') + v('非流动资产合计'), 2))
        a_tot = v('资产总计')
        
    if l_tot == 0 and (v('流动负债合计') != 0 or v('非流动负债合计') != 0):
        update_or_add('负债合计', round(v('流动负债合计') + v('非流动负债合计'), 2))
        l_tot = v('负债合计')

    # 3. 终极勾稽推算
    if a_tot == 0 and l_tot != 0 and e_tot != 0: update_or_add('资产总计', round(l_tot + e_tot, 2))
    elif l_tot == 0 and a_tot != 0 and e_tot != 0: update_or_add('负债合计', round(a_tot - e_tot, 2))
    elif e_tot == 0 and a_tot != 0 and l_tot != 0: update_or_add('所有者权益合计', round(a_tot - l_tot, 2))

# ==========================================
# 4. 主程序 & UI 渲染 (保证 UI 完全一致)
# ==========================================
st.title("🛡️ Fred ETL V1.4 (结构化列示)")
up = st.file_uploader("上传报表文件", type=['xlsx', 'xls', 'pdf', 'csv'])

if up:
    try:
        raw = load_file(up)
        hits, res = {}, []
        
        # 数据提取
        for k in STANDARD_MAP:
            v_pre, r1, c1 = grid_search(raw, k, "期初")
            v_cur, r2, c2 = grid_search(raw, k, "期末")
            hits[(k, "期初")], hits[(k, "期末")] = (r1, c1, v_pre), (r2, c2, v_cur)
            if v_pre != 0 or v_cur != 0:
                res.append({"标准科目": k, "期初余额": v_pre, "期末余额": v_cur})
                
        df_clean = pd.DataFrame(res).fillna(0.0)
        
        # 执行二次计算与勾稽
        err_msg, err_coords = [], []
        if not df_clean.empty:
            for p in ['期初', '期末']:
                calculate_net_and_totals(df_clean, p)
                
                # 记录失衡错误
                val_a = df_clean.loc[df_clean['标准科目'] == '资产总计', f"{p}余额"].values[0] if '资产总计' in df_clean['标准科目'].values else 0.0
                val_l = df_clean.loc[df_clean['标准科目'] == '负债合计', f"{p}余额"].values[0] if '负债合计' in df_clean['标准科目'].values else 0.0
                val_e = df_clean.loc[df_clean['标准科目'] == '所有者权益合计', f"{p}余额"].values[0] if '所有者权益合计' in df_clean['标准科目'].values else 0.0
                
                if abs(round(val_a - (val_l + val_e), 2)) > 0.01:
                    err_msg.append(f"{p}资产负债表失衡")
                    for n in ['资产总计', '负债合计', '所有者权益合计']:
                        if (n, p) in hits and hits[(n, p)][0] != -1:
                            err_coords.append((hits[(n, p)][0], hits[(n, p)][1]))

            # 强制标准排序
            order = list(STANDARD_MAP.keys())
            df_clean['_sort'] = df_clean['标准科目'].apply(lambda x: order.index(x) if x in order else 999)
            df_clean = df_clean.sort_values('_sort').drop('_sort', axis=1).reset_index(drop=True)

        # UI 构建
        tab1, tab2 = st.tabs(["📋 标准化清单与勾稽", "👁️ 开发者透视"])
        
        with tab1:
            st.subheader("📑 资产负债表结构化数据")
            if not df_clean.empty:
                st.dataframe(
                    df_clean.style.format({"期初余额": "{:,.2f}", "期末余额": "{:,.2f}"})
                                 .apply(lambda r: ['background-color: #f8d7da' if any(e[:2] in r['标准科目'] for e in err_msg) else '']*3, axis=1), 
                    width='stretch'
                )
            else:
                st.warning("未能提取到任何有效科目。")
            
            st.markdown("---")
            st.subheader("⚖️ 勾稽关系核算台账 (资产总计 = 负债合计 + 所有者权益合计)")
            
            if not df_clean.empty:
                for p in ['期初', '期末']:
                    col = f"{p}余额"
                    if col not in df_clean.columns: continue
                    v_a = df_clean.loc[df_clean['标准科目'] == '资产总计', col].sum()
                    v_l = df_clean.loc[df_clean['标准科目'] == '负债合计', col].sum()
                    v_e = df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col].sum()
                    diff = round(v_a - (v_l + v_e), 2)
                    
                    st.markdown(f"**【{p}情况】**")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("资产总计", f"{v_a:,.2f}")
                    c2.metric("负债合计", f"{v_l:,.2f}")
                    c3.metric("所有者权益合计", f"{v_e:,.2f}")
                    c4.metric("差额 (资产 - 负债与权益)", f"{diff:,.2f}", delta="平齐" if abs(diff) <= 0.01 else "失衡", delta_color="off" if abs(diff) <= 0.01 else "inverse")

            for m in err_msg: st.error(m)
            if not err_msg and not df_clean.empty: st.success("✅ 精度勾稽对账全部通过")
            
            if not df_clean.empty:
                out = io.BytesIO()
                with pd.ExcelWriter(out) as w: df_clean.to_excel(w, index=False)
                st.download_button("📥 下载标准化 XLSX", out.getvalue(), "Standard_Financial_Report.xlsx")

        with tab2:
            def style_raw(x):
                s = pd.DataFrame('', index=x.index, columns=x.columns)
                for (k, p), (r, col, v) in hits.items():
                    if r != -1: s.iloc[r, col] = 'background-color: #fff3cd; border: 1px solid orange'
                for (r, col) in err_coords:
                    if r != -1: s.iloc[r, col] = 'background-color: #f8d7da; border: 2px solid red'
                return s
            
            raw_display = raw.copy()
            raw_display.columns = raw_display.columns.astype(str)
            raw_display = raw_display.astype(str).replace('nan', '')
            st.dataframe(raw_display.style.apply(style_raw, axis=None), width='stretch')
            
    except Exception as e:
        if isinstance(e, ImportError) and "xlrd" in str(e):
            st.error(e)
        else:
            st.error(f"解析文件时发生错误: {e}")