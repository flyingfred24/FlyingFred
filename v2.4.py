import streamlit as st
import pandas as pd
import re
import io
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode, DataReturnMode

st.set_page_config(page_title="Fred ETL V2.4 (精细算力与锚定版)", layout="wide")

# ==========================================
# 1. 核心数据字典
# ==========================================
BS_STANDARD_MAP = {
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
    
    # 💡 优化 1：彻底打补丁，涵盖所有可能的全角半角及“与/和”连接词
    "所有者权益合计": [
        "所有者权益合计", "股东权益合计", "所有者权益总计", "股东权益总计",
        "所有者权益（或股东权益）合计", "所有者权益(或股东权益)合计",
        r"所有者权益.*或.*股东权益.*[合总]计", r"所有者权益.*或股东权益.*合计", 
        r"所有者权益.*[合总]", r"股东权益.*[合总]", r"所有者权益.*或.*",
        "Total equity", "Total shareholders' equity"
    ],
    "负债和所有者权益总计": [
        "负债和所有者权益总计", "负债和所有者权益合计", "负债及股东权益总计", 
        "负债和所有者权益（或股东权益）总计", "负债和所有者权益(或股东权益)总计",
        "负债与所有者权益（或股东权益）合计", "负债与所有者权益(或股东权益)合计",
        "负债及所有者权益（或股东权益）合计", "负债及所有者权益合计",
        r"负债.*和.*所有者权益.*总计", r"负债.*所有者权益.*或.*股东权益.*[合总]计",
        "Total liabilities and equity"
    ]
}

PL_STANDARD_MAP = {
    "营业收入": ["一、营业收入", "^营业收入$", "营业收入合计"], 
    "主营业务收入": ["其中：主营业务收入", "其中:主营业务收入", "主营业务收入"], 
    "其他业务收入": ["其中：其他业务收入", "其中:其他业务收入", "其他业务收入"],
    
    "营业成本": ["减：营业成本", "减:营业成本", "^营业成本$", "营业成本合计"], 
    "主营业务成本": ["其中：主营业务成本", "其中:主营业务成本", "主营业务成本"], 
    "其他业务成本": ["其中：其他业务成本", "其中:其他业务成本", "其他业务成本"],
    
    "税金及附加": ["税金及附加", "营业税金及附加"],
    "消费税": ["其中：消费税", "其中:消费税", "消费税"], 
    "营业税": ["其中：营业税", "其中:营业税", "营业税"], 
    "城市维护建设税": ["其中：城市维护建设税", "其中:城市维护建设税", "城市维护建设税"], 
    "资源税": ["其中：资源税", "其中:资源税", "资源税"], 
    "教育费附加": ["其中：教育费附加", "其中:教育费附加", "教育费附加"], 
    "城镇土地使用税": ["其中：城镇土地使用税", "其中:城镇土地使用税", "城镇土地使用税"],
    "房产税": ["其中：房产税", "其中:房产税", "房产税"],
    "车船税": ["其中：车船税", "其中:车船税", "车船税"],
    "印花税": ["其中：印花税", "其中:印花税", "印花税"],
    "销售费用": ["销售费用", "营业费用"],
    "广告及宣传费": ["其中：广告费和业务宣传费", "其中:广告费和业务宣传费", "广告费和业务宣传费", "其中：广告费", "其中:广告费", "其中：业务宣传费", "其中:业务宣传费", "广告费", "业务宣传费", "广告及宣传费"],
    "商品维修费": ["其中：商品维修费", "其中:商品维修费", "商品维修费"],
    "运输费": ["其中：运输费", "其中:运输费", "运输费"],
    "包装费": ["其中：包装费", "其中:包装费", "包装费"],
    "管理费用": ["管理费用"],
    "开办费": ["其中：开办费", "其中:开办费", "开办费"], 
    "业务招待费": ["其中：业务招待费", "其中:业务招待费", "业务招待费"],
    "办公费": ["其中：办公费", "其中:办公费", "办公费"],
    "折旧及摊销": ["其中：折旧费", "其中：摊销费", "折旧及摊销", "折旧费"],
    "研发费用": ["研发费用"],
    "财务费用": ["财务费用"],
    "利息费用": ["其中：利息费用", "其中:利息费用", "利息费用", "利息支出"], 
    "利息收入": ["其中：利息收入", "其中:利息收入", "利息收入"], 
    "汇兑净损失": ["其中：汇兑净损失", "其中:汇兑净损失", "汇兑净损失", "汇兑损益"], 
    "手续费": ["其中：手续费", "其中:手续费", "手续费"],
    "其他收益": ["加：其他收益", "加:其他收益", "其他收益"],
    "投资收益": ["加：投资收益", "加:投资收益", "投资收益"],
    "对联营企业和合营企业的投资收益": ["其中：对联营企业和合营企业的投资收益", "其中:对联营企业和合营企业的投资收益"], 
    "公允价值变动收益": ["加：公允价值变动收益", "加:公允价值变动收益", "公允价值变动收益"],
    "信用减值损失": ["减：信用减值损失", "信用减值损失"],
    "资产减值损失": ["减：资产减值损失", "资产减值损失"],
    "资产处置收益": ["加：资产处置收益", "资产处置收益"],
    "营业利润": ["二、营业利润", "营业利润"],
    "营业外收入": ["加：营业外收入", "加:营业外收入", "营业外收入"],
    "非流动资产处置利得": ["其中：非流动资产处置利得", "其中:非流动资产处置利得"], 
    "营业外支出": ["减：营业外支出", "减:营业外支出", "营业外支出"],
    "非流动资产处置损失": ["其中：非流动资产处置损失", "其中:非流动资产处置损失"], 
    "利润总额": ["三、利润总额", "利润总额"],
    "所得税费用": ["减：所得税费用", "减:所得税费用", "所得税费用"],
    "净利润": ["四、净利润", "净利润"],
    "持续经营净利润": ["（一）持续经营净利润", "(一)持续经营净利润", "持续经营净利润"],
    "终止经营净利润": ["（二）终止经营净利润", "(二)终止经营净利润", "终止经营净利润"],
    "归属于母公司所有者的净利润": ["归属于母公司所有者的净利润", "归属于母公司股东的净利润"],
    "少数股东损益": ["少数股东损益"],
    "综合收益总额": ["五、综合收益总额", "综合收益总额"]
}

BS_COL_MAP = {
    "P2": ["期末", "本期", "本年余额", "期末数", "期末余额", "本期数", "本期金额", "本年期末", "Ending", "Closing balance"], 
    "P1": ["期初", "年初", "上年余额", "期初数", "期初余额", "年初数", "年初余额", "上年年末余额", "上期年末余额", "上年数", "上期数", "上年同期", "Opening", "Beginning balance"]
}

PL_COL_MAP = {
    "P1": ["本期", "本月", "本月数", "本期数", "本期金额", "本月金额", "本期发生额"], 
    "P2": ["累计", "本年累计", "本期累计", "本年累计数", "本期累计数", "本期累积数", "本年累积数", "上期金额", "上期发生额", "上期数"]
}

# ==========================================
# 2. V1.3.3 原味处理函数
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
            raise ImportError("缺少 `xlrd` 库。\n终端输入: `pip install xlrd`\n或者把文件另存为 .xlsx")
    elif ext == 'pdf':
        import pdfplumber
        with pdfplumber.open(file) as pdf:
            return pd.concat([pd.DataFrame(t) for p in pdf.pages for t in p.extract_tables() if t])
    return pd.read_csv(file, header=None)

def grid_search(df, row_key, period_key, table_type="BS", pl_used_cells=None):
    map_dict = BS_STANDARD_MAP if table_type == "BS" else PL_STANDARD_MAP
    col_dict = BS_COL_MAP if table_type == "BS" else PL_COL_MAP
    row_aliases = map_dict[row_key]
    col_aliases = col_dict[period_key]
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
                
                if table_type == "BS":
                    exclude_flags = False
                    for ex in ["其中", "减值", "准备", "跌价", "折旧", "摊销", "清理", "减:", "减：", "加:", "加：", "加项", "减项", "+"]:
                        if ex in raw and ex not in str(row_aliases):
                            exclude_flags = True
                            break
                    if exclude_flags: continue
                    
                    if (raw.startswith("-") or raw.startswith("减")) and not any(kw in str(row_aliases) for kw in ["折旧", "摊销", "准备", "坏账", "减:"]):
                        continue
                else:
                    if "其中" in raw and "其中" not in str(row_aliases): continue
                    if row_key == "营业税" and "附加" in raw: continue
                    if row_key == "消费税" and "附加" in raw: continue
                    
                    if row_key == "营业收入" and any(x in raw for x in ["主营", "其他", "外"]): continue
                    if row_key == "营业成本" and any(x in raw for x in ["主营", "其他", "外"]): continue
                    if row_key == "主营业务收入" and "成本" in raw: continue
                    if row_key == "主营业务成本" and "收入" in raw: continue
                    if row_key == "其他业务收入" and "成本" in raw: continue
                    if row_key == "其他业务成本" and "收入" in raw: continue
                    
                if row_key == "其他权益工具" and "投资" in raw: continue
                
                for row_offset in [0, 1]: 
                    check_r = r + row_offset
                    if check_r >= len(df): continue
                    for tc in [tc for tc in target_cols if tc >= c]:
                        for off in [0, 1]:  
                            if tc + off < len(df.columns):
                                if table_type == "PL" and pl_used_cells is not None and (check_r, tc+off) in pl_used_cells: continue
                                v = clean_num(df.iloc[check_r, tc+off])
                                if v is not None: return v, check_r, tc+off
                    for bc in range(c + 1, len(df.columns)):
                        if table_type == "PL" and pl_used_cells is not None and (check_r, bc) in pl_used_cells: continue
                        v = clean_num(df.iloc[check_r, bc])
                        if v is not None: return v, check_r, bc
    return 0.0, -1, -1

# ==========================================
# 3. 业务逻辑计算 (V1.3.3 原汁原味)
# ==========================================
def calculate_net_and_totals_bs(df_clean, col_key, c1_name, c2_name):
    if df_clean.empty: return
    
    def v(n):
        res_val = df_clean.loc[df_clean['标准科目']==n, col_key]
        return res_val.values[0] if not res_val.empty else 0.0
        
    def calc_net(gross_key, contra_keys, net_key):
        gross_val = v(gross_key)
        contra_val = sum([abs(v(k)) for k in contra_keys]) 
        net_extracted = v(net_key)
        
        if net_extracted == 0 and gross_val != 0:
            net_calc = round(gross_val - contra_val, 2)
            if net_key in df_clean['标准科目'].values: 
                df_clean.loc[df_clean['标准科目'] == net_key, col_key] = net_calc
            else:
                val1 = net_calc if col_key == c1_name else 0.0
                val2 = net_calc if col_key == c2_name else 0.0
                if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = [net_key, val1, val2]
                else: df_clean.loc[len(df_clean)] = [net_key, net_calc]

    calc_net("固定资产", ["累计折旧", "固定资产减值准备"], "固定资产净额")
    calc_net("无形资产", ["累计摊销", "无形资产减值准备"], "无形资产净额")
    calc_net("存货", ["存货跌价准备"], "存货净额")
    calc_net("应收账款", ["坏账准备"], "应收账款净额")
    calc_net("长期股权投资", ["长期股权投资减值准备"], "长期股权投资净额")

    a_curr, a_non = v('流动资产合计'), v('非流动资产合计')
    if v('资产总计') == 0 and (a_curr != 0 or a_non != 0):
        a_total_calc = round(a_curr + a_non, 2)
        if '资产总计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '资产总计', col_key] = a_total_calc
        else: 
            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = ['资产总计', a_total_calc if col_key == c1_name else 0.0, a_total_calc if col_key == c2_name else 0.0]
            else: df_clean.loc[len(df_clean)] = ['资产总计', a_total_calc]

    l_curr, l_non = v('流动负债合计'), v('非流动负债合计')
    if v('负债合计') == 0 and (l_curr != 0 or l_non != 0):
        l_total_calc = round(l_curr + l_non, 2)
        if '负债合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '负债合计', col_key] = l_total_calc
        else: 
            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = ['负债合计', l_total_calc if col_key == c1_name else 0.0, l_total_calc if col_key == c2_name else 0.0]
            else: df_clean.loc[len(df_clean)] = ['负债合计', l_total_calc]

    a_total, l_total, e_total = v('资产总计'), v('负债合计'), v('所有者权益合计')
    
    if a_total == 0 and l_total != 0 and e_total != 0:
        a_total = round(l_total + e_total, 2)
        if '资产总计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '资产总计', col_key] = a_total
        else: 
            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = ['资产总计', a_total if col_key == c1_name else 0.0, a_total if col_key == c2_name else 0.0]
            else: df_clean.loc[len(df_clean)] = ['资产总计', a_total]
    
    elif l_total == 0 and a_total != 0 and e_total != 0:
        l_total = round(a_total - e_total, 2)
        if '负债合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '负债合计', col_key] = l_total
        else: 
            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = ['负债合计', l_total if col_key == c1_name else 0.0, l_total if col_key == c2_name else 0.0]
            else: df_clean.loc[len(df_clean)] = ['负债合计', l_total]
    
    elif e_total == 0 and a_total != 0 and l_total != 0:
        e_total = round(a_total - l_total, 2)
        if '所有者权益合计' in df_clean['标准科目'].values: df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col_key] = e_total
        else: 
            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = ['所有者权益合计', e_total if col_key == c1_name else 0.0, e_total if col_key == c2_name else 0.0]
            else: df_clean.loc[len(df_clean)] = ['所有者权益合计', e_total]

# ==========================================
# 4. 主程序 & UI 渲染
# ==========================================
st.sidebar.title("🛠️ 配置面板")
table_type = st.sidebar.radio("选择要解析的报表类型:", ("资产负债表 (BS)", "利润表 (PL)"))

st.title(f"🛡️ Fred ETL V2.4 - {table_type.split(' ')[0]}")

up = st.file_uploader(f"上传{table_type.split(' ')[0]}文件", type=['xlsx', 'xls', 'pdf', 'csv'])

if up:
    try:
        raw = load_file(up)
        hits, res = {}, []
        
        is_bs = "BS" in table_type
        current_map = BS_STANDARD_MAP if is_bs else PL_STANDARD_MAP
        t_type = "BS" if is_bs else "PL"
        
        c1_name = "期初余额" if is_bs else "本期金额"
        c2_name = "期末余额" if is_bs else "累计金额"
        
        pl_used_p1 = set() if not is_bs else None
        pl_used_p2 = set() if not is_bs else None
        
        for k in current_map:
            v_1, r1, col1 = grid_search(raw, k, "P1", t_type, pl_used_p1)
            if not is_bs and r1 != -1: pl_used_p1.add((r1, col1))
            
            v_2, r2, col2 = grid_search(raw, k, "P2", t_type, pl_used_p2)
            if not is_bs and r2 != -1: pl_used_p2.add((r2, col2))
            
            hits[(k, "P1")], hits[(k, "P2")] = (r1, col1, v_1), (r2, col2, v_2)
            
            if v_1 != 0 or v_2 != 0:
                res.append({"标准科目": k, c1_name: v_1, c2_name: v_2})
                
        df_clean = pd.DataFrame(res)
        if not df_clean.empty:
            df_clean[c1_name] = pd.to_numeric(df_clean[c1_name], errors='coerce').fillna(0.0)
            df_clean[c2_name] = pd.to_numeric(df_clean[c2_name], errors='coerce').fillna(0.0)
        
        active_cols = [c1_name, c2_name]
        if not df_clean.empty:
            if c2_name in df_clean.columns and (df_clean[c2_name] == 0).all():
                df_clean = df_clean.drop(columns=[c2_name])
                active_cols = [c1_name]
                hits = {k: v for k, v in hits.items() if k[1] != "P2"}
            elif c1_name in df_clean.columns and (df_clean[c1_name] == 0).all():
                df_clean = df_clean.drop(columns=[c1_name])
                active_cols = [c2_name]
                hits = {k: v for k, v in hits.items() if k[1] != "P1"}
        
        err_msg, err_coords = [], []
        
        if not df_clean.empty:
            if is_bs:
                # 资产负债表 维持纯粹的 V1.3.3
                for col_key in active_cols:
                    p_id = "P1" if col_key == c1_name else "P2"
                    calculate_net_and_totals_bs(df_clean, col_key, c1_name, c2_name)
                    
                    val_a = df_clean.loc[df_clean['标准科目'] == '资产总计', col_key].values[0] if '资产总计' in df_clean['标准科目'].values else 0.0
                    val_l = df_clean.loc[df_clean['标准科目'] == '负债合计', col_key].values[0] if '负债合计' in df_clean['标准科目'].values else 0.0
                    val_e = df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col_key].values[0] if '所有者权益合计' in df_clean['标准科目'].values else 0.0
                    
                    if abs(round(val_a - (val_l + val_e), 2)) > 0.01:
                        err_msg.append(f"【{col_key}】资产负债表失衡")
                        for n in ['资产总计', '负债合计', '所有者权益合计']:
                            if (n, p_id) in hits and hits[(n, p_id)][0] != -1:
                                err_coords.append((hits[(n, p_id)][0], hits[(n, p_id)][1]))
            else:
                for col_key in active_cols:
                    p_id = "P1" if col_key == c1_name else "P2"
                    
                    def get_pl_v(k):
                        match = df_clean.loc[df_clean['标准科目'] == k, col_key]
                        return match.values[0] if not match.empty else 0.0
                        
                    def update_or_add_pl(k, val):
                        if k in df_clean['标准科目'].values:
                            df_clean.loc[df_clean['标准科目'] == k, col_key] = val
                        else:
                            val1 = val if col_key == c1_name else 0.0
                            val2 = val if col_key == c2_name else 0.0
                            if c2_name in df_clean.columns: df_clean.loc[len(df_clean)] = [k, val1, val2]
                            else: df_clean.loc[len(df_clean)] = [k, val]
                    
                    # 💡 优化 2：利用子项反推父项，严格贯彻“主营+其他=营业”
                    if get_pl_v("营业收入") == 0:
                        sub_rev = get_pl_v("主营业务收入") + get_pl_v("其他业务收入")
                        if sub_rev != 0: update_or_add_pl("营业收入", round(sub_rev, 2))
                        
                    if get_pl_v("营业成本") == 0:
                        sub_cost = get_pl_v("主营业务成本") + get_pl_v("其他业务成本")
                        if sub_cost != 0: update_or_add_pl("营业成本", round(sub_cost, 2))
                        
                    if get_pl_v("税金及附加") == 0:
                        tax_subs = ["消费税", "营业税", "城市维护建设税", "资源税", "教育费附加", "城镇土地使用税", "房产税", "车船税", "印花税"]
                        tax_sum = sum(get_pl_v(t) for t in tax_subs)
                        if tax_sum != 0: update_or_add_pl("税金及附加", round(tax_sum, 2))
                    
                    tot_profit = get_pl_v("利润总额")
                    tax_exp = get_pl_v("所得税费用")
                    net_profit = get_pl_v("净利润")
                    
                    if tot_profit != 0 and net_profit != 0:
                        diff = abs(round(tot_profit - tax_exp - net_profit, 2))
                        if diff > 0.01:
                            err_msg.append(f"【{col_key}】存在差异(利润总额-所得税与净利润不符)")
                            for n in ['利润总额', '所得税费用', '净利润']:
                                if (n, p_id) in hits and hits[(n, p_id)][0] != -1: 
                                    err_coords.append((hits[(n, p_id)][0], hits[(n, p_id)][1]))

            order = list(current_map.keys())
            df_clean['_sort'] = df_clean['标准科目'].apply(lambda x: order.index(x) if x in order else 999)
            df_clean = df_clean.sort_values('_sort').drop('_sort', axis=1).reset_index(drop=True)

        tab1, tab2 = st.tabs(["📋 标准化清单与勾稽", "👁️ 开发者透视"])
        
        with tab1:
            st.subheader(f"📑 {table_type.split(' ')[0]}结构化数据 (拖选计算 Sum, Avg)")
            if not df_clean.empty:
                gb = GridOptionsBuilder.from_dataframe(df_clean)
                gb.configure_default_column(flex=1, minWidth=150)
                gb.configure_selection('multiple', use_checkbox=True)
                gb.configure_grid_options(enableRangeSelection=True, copyHeadersToClipboard=False)
                
                for col in active_cols:
                    gb.configure_column(col, type=["numericColumn"], valueFormatter="x != null ? Number(x).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : ''")
                
                cellsytle_jscode = JsCode("""
                function(params) {
                    var errMsgs = %s;
                    var subject = params.data.标准科目;
                    for(var i=0; i<errMsgs.length; i++){
                        if(errMsgs[i].indexOf(subject) !== -1 || errMsgs[i].indexOf(subject.substring(0,2)) !== -1) {
                            return {'backgroundColor': '#f8d7da', 'color': '#721c24'};
                        }
                    }
                    return null;
                };
                """ % (str(err_msg)))
                gb.configure_column("标准科目", cellStyle=cellsytle_jscode)

                grid_response = AgGrid(
                    df_clean,
                    gridOptions=gb.build(),
                    allow_unsafe_jscode=True,
                    theme='streamlit',
                    height=450,
                    fit_columns_on_grid_load=True,
                    update_mode=GridUpdateMode.SELECTION_CHANGED, 
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED
                )
                
                selected_rows = grid_response['selected_rows']
                if isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
                    stat_html = "<div style='display:flex; justify-content:flex-end; gap:20px; padding:10px; background-color:#f0f2f6; border-radius:5px; margin-bottom:20px; font-weight:bold; color:#31333F;'>"
                    stat_html += f"<span>📉 选中行数: <span style='color:#0068c9;'>{len(selected_rows)}</span></span>"
                    for col in active_cols:
                        if col in selected_rows.columns:
                            val_sum = pd.to_numeric(selected_rows[col], errors='coerce').sum()
                            val_avg = pd.to_numeric(selected_rows[col], errors='coerce').mean()
                            stat_html += f"<span>Σ {col} 合计: <span style='color:#0068c9;'>{val_sum:,.2f}</span></span>"
                            stat_html += f"<span>μ 平均: <span style='color:#0068c9;'>{val_avg:,.2f}</span></span>"
                    stat_html += "</div>"
                    st.markdown(stat_html, unsafe_allow_html=True)
            else:
                st.warning("未能提取到任何有效科目。")
            
            st.markdown("---")
            if is_bs:
                st.subheader("⚖️ 勾稽关系核算台账 (资产总计 = 负债合计 + 所有者权益合计)")
                if not df_clean.empty:
                    for col_key in active_cols:
                        v_a = df_clean.loc[df_clean['标准科目'] == '资产总计', col_key].sum()
                        v_l = df_clean.loc[df_clean['标准科目'] == '负债合计', col_key].sum()
                        v_e = df_clean.loc[df_clean['标准科目'] == '所有者权益合计', col_key].sum()
                        diff = round(v_a - (v_l + v_e), 2)
                        
                        st.markdown(f"**【{col_key}】**")
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("资产总计", f"{v_a:,.2f}")
                        col2.metric("负债合计", f"{v_l:,.2f}")
                        col3.metric("所有者权益合计", f"{v_e:,.2f}")
                        col4.metric("差额", f"{diff:,.2f}", delta="平齐" if abs(diff) <= 0.01 else "失衡", delta_color="off" if abs(diff) <= 0.01 else "inverse")
            else:
                st.subheader("⚖️ 利润表核心校验与明细台账")
                if not df_clean.empty:
                    for col_key in active_cols:
                        v_tot = df_clean.loc[df_clean['标准科目'] == '利润总额', col_key].sum()
                        v_tax = df_clean.loc[df_clean['标准科目'] == '所得税费用', col_key].sum()
                        v_net = df_clean.loc[df_clean['标准科目'] == '净利润', col_key].sum()
                        diff = round(v_tot - v_tax - v_net, 2)
                        
                        st.markdown(f"**【{col_key}大类校验】**")
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("利润总额", f"{v_tot:,.2f}")
                        col2.metric("所得税费用", f"{v_tax:,.2f}")
                        col3.metric("净利润", f"{v_net:,.2f}")
                        col4.metric("推算差额", f"{diff:,.2f}", delta="平齐" if abs(diff) <= 0.01 else "失衡", delta_color="off" if abs(diff) <= 0.01 else "inverse")
                        
                        st.markdown(f"**【{col_key}科目穿透】**")
                        for main_item, sub_items in [
                            ("营业收入", ["主营业务收入", "其他业务收入"]),
                            ("营业成本", ["主营业务成本", "其他业务成本"]),
                            ("税金及附加", ["消费税", "营业税", "城市维护建设税", "资源税", "教育费附加", "城镇土地使用税", "房产税", "车船税", "印花税"]),
                            ("销售费用", ["广告及宣传费", "商品维修费", "运输费", "包装费"]),
                            ("管理费用", ["开办费", "业务招待费", "办公费", "折旧及摊销"]),
                            ("财务费用", ["利息费用", "利息收入", "汇兑净损失", "手续费"])
                        ]:
                            v_main = df_clean.loc[df_clean['标准科目'] == main_item, col_key].sum()
                            v_subs = sum([df_clean.loc[df_clean['标准科目'] == sub, col_key].sum() for sub in sub_items])
                            if v_main != 0 or v_subs != 0:
                                st.caption(f"🔹 **{main_item}** 总计: **{v_main:,.2f}** ｜ 其中已识别明细汇总: **{v_subs:,.2f}**")

            for m in err_msg: st.error(m)
            if not err_msg and not df_clean.empty: st.success("✅ 精度勾稽对账全部通过")
            
            if not df_clean.empty:
                out = io.BytesIO()
                with pd.ExcelWriter(out) as w: df_clean.to_excel(w, index=False)
                st.download_button("📥 下载标准化 XLSX", out.getvalue(), f"Standard_{t_type}_Report.xlsx")

        with tab2:
            st.subheader("👁️ 开发者透视")
            raw_display = raw.copy()
            raw_display.columns = raw_display.columns.astype(str)
            raw_display = raw_display.astype(str).replace('nan', '')
            
            hit_cells = []
            for (k, p), (r, col, v) in hits.items():
                if r != -1: hit_cells.append({"row": r, "col": str(col), "type": "hit"})
            for (r, col) in err_coords:
                if r != -1: hit_cells.append({"row": r, "col": str(col), "type": "err"})
                
            raw_jscode = JsCode("""
            function(params) {
                var hitCells = %s;
                var rowIndex = params.node.rowIndex;
                var colId = params.colDef.field;
                
                for(var i=0; i<hitCells.length; i++) {
                    if (hitCells[i].row === rowIndex && hitCells[i].col === colId) {
                        if (hitCells[i].type === 'err') {
                            return {'backgroundColor': '#f8d7da', 'border': '2px solid red'};
                        } else {
                            return {'backgroundColor': '#fff3cd', 'border': '1px solid orange'};
                        }
                    }
                }
                return null;
            };
            """ % str(hit_cells))

            raw_gb = GridOptionsBuilder.from_dataframe(raw_display)
            raw_gb.configure_grid_options(enableRangeSelection=True)
            for col in raw_display.columns:
                raw_gb.configure_column(col, cellStyle=raw_jscode)
                
            AgGrid(
                raw_display,
                gridOptions=raw_gb.build(),
                allow_unsafe_jscode=True,
                theme='streamlit',
                height=600,
                fit_columns_on_grid_load=False
            )
            
    except Exception as e:
        if isinstance(e, ImportError) and "xlrd" in str(e):
            st.error(e)
        else:
            st.error(f"解析文件时发生错误: {e}")