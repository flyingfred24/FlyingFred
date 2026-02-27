import streamlit as st
import pandas as pd
import re
import io

st.set_page_config(page_title="Fred ETL V1.3.2 (结构化列示与勾稽面板)", layout="wide")

# --- 1. 完整版词典 (按标准资产负债表顺序排列) ---
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

# --- 2. 数字化清洗 ---
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
    if ext in ['xls', 'xlsx']:
        xls = pd.read_excel(file, sheet_name=None, header=None)
        return max(xls.values(), key=len)
    elif ext == 'pdf':
        import pdfplumber
        with pdfplumber.open(file) as pdf:
            return pd.concat([pd.DataFrame(t) for p in pdf.pages for t in p.extract_tables() if t])
    return pd.read_csv(file, header=None)

# --- 3. 增强版网格搜索 ---
def grid_search(df, row_key, period):
    row_aliases, col_aliases = STANDARD_MAP[row_key], COL_MAP[period]
    target_cols = []
    
    for r in range(min(20, len(df))):
        for c in range(len(df.columns)):
            cell_txt = str(df.iloc[r, c]).replace("\n", "").replace(" ", "").replace("　", "").lower()
            if not cell_txt or cell_txt == 'nan': continue
            for alias in col_aliases:
                al = alias.lower()
                if (cell_txt in al and len(cell_txt) >= 2) or (al in cell_txt):
                    if c not in target_cols: target_cols.append(c)
                    break
                    
    if not target_cols: return 0.0, -1, -1

    for r in range(len(df)):
        for c in range(len(df.columns)):
            raw = str(df.iloc[r,c]).replace("\n","").replace(" ","").replace("　","").lower()
            if not raw or raw == 'nan': continue
            
            is_match = False
            for a in row_aliases:
                al = a.lower() if not a.startswith(r'^') else a
                if a.startswith(r'^') or '.*' in a:
                    if re.search(al, raw):
                        is_match = True
                        break
                else:
                    if al in raw:  
                        is_match = True
                        break

            if is_match:
                if "流动" in raw and "流动" not in str(row_aliases): continue
                if "非流动" in raw and "非" not in str(row_aliases): continue
                
                exclude_flags = False
                for ex in ["其中", "减值", "准备", "跌价", "折旧", "摊销", "清理", "减:", "减：", "加:", "加：", "加项", "减项", "+"]:
                    if ex in raw and ex not in str(row_aliases):
                        exclude_flags = True
                        break
                if exclude_flags: continue
                
                if (raw.startswith("-") or raw.startswith("减")) and not any(kw in str(row_aliases) for kw in ["折旧", "摊销", "准备", "坏账", "减:"]):
                    continue
                
                if row_key == "其他权益工具" and "投资" in raw: continue
                
                for row_offset in [0, 1]: 
                    check_r = r + row_offset
                    if check_r >= len(df): continue
                    valid_target_cols = [tc for tc in target_cols if tc >= c] 
                    for tc in valid_target_cols:
                        for off in [0, 1]:  
                            if tc + off < len(df.columns):
                                v = clean_num(df.iloc[check_r, tc+off])
                                if v is not None:
                                    return v, check_r, tc+off
                    for bc in range(c + 1, len(df.columns)):
                        v = clean_num(df.iloc[check_r, bc])
                        if v is not None:
                            return v, check_r, bc
    return 0.0, -1, -1

# --- 4. 主程序 ---
st.title("🛡️ Fred 财务标准化工厂 V1.3.2 (结构化列示)")
up = st.file_uploader("上传报表文件", type=['xlsx', 'xls', 'pdf', 'csv'])

if up:
    try:
        raw = load_file(up)
        res, hits = [], {}
        
        for k in STANDARD_MAP:
            v_pre, r1, c1 = grid_search(raw, k, "期初")
            v_cur, r2, c2 = grid_search(raw, k, "期末")
            hits[(k, "期初")], hits[(k, "期末")] = (r1, c1, v_pre), (r2, c2, v_cur)
            
            if v_pre != 0 or v_cur != 0:
                res.append({"标准科目": k, "期初余额": v_pre, "期末余额": v_cur})
        
        df_clean = pd.DataFrame(res)
        if not df_clean.empty:
            df_clean["期初余额"] = pd.to_numeric(df_clean["期初余额"], errors='coerce').fillna(0.0)
            df_clean["期末余额"] = pd.to_numeric(df_clean["期末余额"], errors='coerce').fillna(0.0)

        err_msg, err_coords = [], []
        
        def validate_bs(p_key):
            if df_clean.empty: return
            col_name = f"{p_key}余额"
            
            def v(n):
                res_val = df_clean.loc[df_clean['标准科目']==n, col_name]
                return res_val.values[0] if not res_val.empty else 0.0
                
            c = lambda n: (hits[(n, p_key)][0], hits[(n, p_key)][1])
            
            def calc_net(gross_key, contra_keys, net_key):
                gross_val = v(gross_key)
                contra_val = sum([abs(v(k)) for k in contra_keys]) 
                net_extracted = v(net_key)
                
                if net_extracted == 0 and gross_val != 0:
                    net_calc = round(gross_val - contra_val, 2)
                    if net_key in df_clean['标准科目'].values: 
                        df_clean.loc[df_clean['标准科目'] == net_key, col_name] = net_calc
                    else: 
                        df_clean.loc[len(df_clean)] = [net_key, net_calc if p_key == '期初' else 0.0, net_calc if p_key == '期末' else 0.0]

            calc_net("固定资产", ["累计折旧", "固定资产减值准备"], "固定资产净额")
            calc_net("无形资产", ["累计摊销", "无形资产减值准备"], "无形资产净额")
            calc_net("存货", ["存货跌价准备"], "存货净额")
            calc_net("应收账款", ["坏账准备"], "应收账款净额")
            calc_net("长期股权投资", ["长期股权投资减值准备"], "长期股权投资净额")

            a_curr, a_non = v('流动资产合计'), v('非流动资产合计')
            if v('资产总计') == 0 and (a_curr != 0 or a_non != 0):
                a_total_calc = round(a_curr + a_non, 2)
                if '资产总计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '资产总计', col_name] = a_total_calc
                else: df_clean.loc[len(df_clean)] = ['资产总计', a_total_calc if p_key == '期初' else 0.0, a_total_calc if p_key == '期末' else 0.0]

            l_curr, l_non = v('流动负债合计'), v('非流动负债合计')
            if v('负债合计') == 0 and (l_curr != 0 or l_non != 0):
                l_total_calc = round(l_curr + l_non, 2)
                if '负债合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '负债合计', col_name] = l_total_calc
                else: df_clean.loc[len(df_clean)] = ['负债合计', l_total_calc if p_key == '期初' else 0.0, l_total_calc if p_key == '期末' else 0.0]

            a_total, l_total, e_total = v('资产总计'), v('负债合计'), v('所有者权益合计')
            
            if a_total == 0 and l_total != 0 and e_total != 0:
                a_total = round(l_total + e_total, 2)
                if '资产总计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '资产总计', col_name] = a_total
                else: df_clean.loc[len(df_clean)] = ['资产总计', a_total if p_key == '期初' else 0.0, a_total if p_key == '期末' else 0.0]
            
            elif l_total == 0 and a_total != 0 and e_total != 0:
                l_total = round(a_total - e_total, 2)
                if '负债合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '负债合计', col_name] = l_total
                else: df_clean.loc[len(df_clean)] = ['负债合计', l_total if p_key == '期初' else 0.0, l_total if p_key == '期末' else 0.0]
            
            elif e_total == 0 and a_total != 0 and l_total != 0:
                e_total = round(a_total - l_total, 2)
                if '所有者权益合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col_name] = e_total
                else: df_clean.loc[len(df_clean)] = ['所有者权益合计', e_total if p_key == '期初' else 0.0, e_total if p_key == '期末' else 0.0]

            final_a, final_l, final_e = v('资产总计'), v('负债合计'), v('所有者权益合计')
            diff = abs(round(final_a - (final_l + final_e), 2))
            if diff > 0.01:
                err_msg.append(f"{p_key}资产负债表失衡")
                for n in ['资产总计', '负债合计', '所有者权益合计']:
                    if n in [k for k, p in hits.keys() if p == p_key]:
                        coord = c(n)
                        if coord[0] != -1: err_coords.append(coord)

        for p in ['期初', '期末']: validate_bs(p)

        # 💡 V1.3.2 核心排序逻辑：按照中国资产负债表标准顺序重排 DataFrame
        if not df_clean.empty:
            standard_order = list(STANDARD_MAP.keys())
            df_clean['_sort'] = df_clean['标准科目'].apply(lambda x: standard_order.index(x) if x in standard_order else 999)
            df_clean = df_clean.sort_values('_sort').drop('_sort', axis=1).reset_index(drop=True)

        tab1, tab2 = st.tabs(["📋 标准化清单与勾稽", "👁️ 开发者透视"])
        
        with tab1:
            st.subheader("📑 资产负债表结构化数据")
            if not df_clean.empty:
                st.dataframe(
                    df_clean.style.format({
                        "期初余额": "{:,.2f}", 
                        "期末余额": "{:,.2f}"
                    }).apply(lambda r: ['background-color: #f8d7da' if any(e[:2] in r['标准科目'] for e in err_msg) else '']*3, axis=1), 
                    width='stretch'
                )
            else:
                st.warning("未能提取到任何有效科目。")
            
            st.markdown("---")
            st.subheader("⚖️ 勾稽关系核算台账 (资产总计 = 负债合计 + 所有者权益合计)")
            
            if not df_clean.empty:
                # 动态获取实际存在的数据列
                periods = [col.replace('余额', '') for col in df_clean.columns if '余额' in col]
                for p in periods:
                    col_name = f"{p}余额"
                    val_a = df_clean.loc[df_clean['标准科目'] == '资产总计', col_name].values[0] if '资产总计' in df_clean['标准科目'].values else 0.0
                    val_l = df_clean.loc[df_clean['标准科目'] == '负债合计', col_name].values[0] if '负债合计' in df_clean['标准科目'].values else 0.0
                    val_e = df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col_name].values[0] if '所有者权益合计' in df_clean['标准科目'].values else 0.0
                    val_le = round(val_l + val_e, 2)
                    diff = round(val_a - val_le, 2)
                    
                    st.markdown(f"**【{p}情况】**")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("资产总计", f"{val_a:,.2f}")
                    col2.metric("负债合计", f"{val_l:,.2f}")
                    col3.metric("所有者权益合计", f"{val_e:,.2f}")
                    col4.metric("差额 (资产 - 负债与权益)", f"{diff:,.2f}", delta="平齐" if abs(diff) <= 0.01 else "失衡", delta_color="off" if abs(diff) <= 0.01 else "inverse")

            for m in err_msg: st.error(m)
            if not err_msg and not df_clean.empty: st.success("✅ 精度勾稽对账全部通过")
            
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
        st.error(f"解析文件时发生严重错误: {e}")