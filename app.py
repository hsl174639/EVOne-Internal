import os
import io
import gc
import zipfile
import warnings
import requests
import pandas as pd
from typing import List
from datetime import datetime
from dotenv import load_dotenv
from jose import JWTError, jwt
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Request
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client

# ReportLab 绘图库
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

warnings.filterwarnings('ignore')
load_dotenv()

app = FastAPI(title="EVOne Internal System API")

# ==========================================
# 1. Base Configuration
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

security = HTTPBearer()
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")
DOCUSEAL_API_KEY = os.environ.get("DOCUSEAL_API_KEY")
DOCUSEAL_BASE = "https://api.docuseal.com"

# 初始化 Supabase Admin (Service Role)
supabase_admin: Client = create_client(
    os.environ.get("SUPABASE_URL"), 
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Auth failed")

def check_is_admin(user_payload: dict) -> bool:
    """
    RBAC 核心逻辑：基于 role_id 判定权限
    1: Admin
    2: Finance
    3: Read Only
    """
    id = user_payload.get("sub")
    if not id:
        return False
        
    try:
        # 将 schema 和 table 替换为你的真实表名
        res = supabase_admin.schema("evone_billing").table("users") \
            .select("role_id") \
            .eq("id", id) \
            .execute()
        
        if res.data and len(res.data) > 0:
            current_role_id = res.data[0].get("role_id")
            
            # 【因果判定】
            # 如果仅限 Admin (1) 可以上传文件：
            return current_role_id == 1
            
            # 如果 Admin (1) 和 Finance (2) 都可以上传文件，请使用以下代码替换上一行：
            # return current_role_id in [1, 2]
            
    except Exception as e:
        print(f"RBAC Query Error: {e}")
        
    return False

# ==========================================
# 2. Page & Config Routes
# ==========================================
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/")
async def serve_billing(request: Request):
    return templates.TemplateResponse(request=request, name="billing.html")

@app.get("/e-sign")
async def serve_signing(request: Request):
    return templates.TemplateResponse(request=request, name="signing.html")

@app.get("/documents")
async def serve_documents(request: Request):
    return templates.TemplateResponse(request=request, name="files.html")

@app.get("/config")
async def get_config():
    return {
        "supabase_url": os.environ.get("SUPABASE_URL"),
        "supabase_key": os.environ.get("SUPABASE_ANON_KEY")
    }

# ==========================================
# 3. Document Management API (RBAC)
# ==========================================
@app.get("/api/list-files")
async def list_files(user: dict = Depends(get_current_user)):
    try:
        res = supabase_admin.storage.from_("Documents").list(path="general")
        files = [f for f in res if f.get("name") and not f.get("name").startswith(".")]
        return {
            "is_admin": check_is_admin(user),
            "files": files
        }
    except Exception as e:
        return {"error": True, "message": str(e)}

@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    # 强制执行 RBAC 校验
    if not check_is_admin(user):
        raise HTTPException(status_code=403, detail="Admins only")
    try:
        file_bytes = await file.read()
        safe_name = "".join([c for c in file.filename if c.isalnum() or c in (" ", "_", ".", "-")]).strip()
        supabase_admin.storage.from_("Documents").upload(
            path=f"general/{safe_name}",
            file=file_bytes,
            file_options={"content-type": file.content_type, "x-upsert": "true"}
        )
        return {"success": True}
    except Exception as e:
        return {"error": True, "message": str(e)}

@app.get("/api/get-file-url/{filename}")
async def get_file_url(filename: str, user: dict = Depends(get_current_user)):
    try:
        res = supabase_admin.storage.from_("Documents").create_signed_url(f"general/{filename}", 3600)
        return {"url": res.get("signedURL")}
    except Exception as e:
        return {"error": True, "message": str(e)}

# ==========================================
# 4. E-Sign & Billing Logic (维持原样)
# ==========================================
# [此处接你代码中已有的 create_signature, get_submissions, process_pdf 等函数...]

@app.post("/api/create-signature")
async def create_signature(data: dict, user: dict = Depends(get_current_user)):
    headers = {"X-Auth-Token": DOCUSEAL_API_KEY, "Content-Type": "application/json"}
    
    # 获取新增的 category 字段
    category = data.get("category", "Internal").strip()
    form_type = data.get("form_type")
    signers_count = str(data.get("signers_count", "1"))
    prefix = data.get("prefix", "").strip()
    
    template_id = None
    submitters = []

    # 按签署人数的极致简化映射逻辑 (1人 ES, 2人 ES+LEW, 3人 Insp+ES+LEW)
    if form_type == "Form A":
        if signers_count == "1":
            template_id = os.environ.get("TEMPLATE_ID_FORM_A_1P")
            submitters = [{"role": "ES", "email": data.get("email_es"), "name": data.get("name_es")}]
        else:
            template_id = os.environ.get("TEMPLATE_ID_FORM_A_2P")
            submitters = [
                {"role": "ES", "email": data.get("email_es"), "name": data.get("name_es")},
                {"role": "LEW", "email": data.get("email_lew"), "name": data.get("name_lew")}
            ]
    elif form_type in ["Form D", "Form 1"]:
        suffix = "FORM_D" if form_type == "Form D" else "FORM_1"
        if signers_count == "3":
            template_id = os.environ.get(f"TEMPLATE_ID_{suffix}_3P")
            submitters = [
                {"role": "Inspector", "email": data.get("email_inspector"), "name": data.get("name_inspector")},
                {"role": "ES", "email": data.get("email_es"), "name": data.get("name_es")},
                {"role": "LEW", "email": data.get("email_lew"), "name": data.get("name_lew")}
            ]
        else:
            template_id = os.environ.get(f"TEMPLATE_ID_{suffix}_2P")
            submitters = [
                {"role": "ES", "email": data.get("email_es"), "name": data.get("name_es")},
                {"role": "LEW", "email": data.get("email_lew"), "name": data.get("name_lew")}
            ]

    if not template_id:
        return {"error": True, "message": "Template mapping failed"}

    # 将 Category 标签强行注入 Document Name
    base_name = f"{prefix} {form_type}" if prefix else form_type
    document_name = f"[{category}] {base_name}"

    payload = {
        "template_id": int(template_id),
        "name": document_name,
        "send_email": True,
        "message": {
            "subject": document_name,
            "body": f"Hello {{submitter.name}},\n\nPlease complete the document via the link: {{submitter.link}}"
        },
        "submitters": submitters
    }
    
    response = requests.post(f"{DOCUSEAL_BASE}/submissions", json=payload, headers=headers)
    return response.json()


@app.get("/api/get-document-download/{sub_id}")
async def get_download(sub_id: str, user: dict = Depends(get_current_user)):
    headers = {"X-Auth-Token": DOCUSEAL_API_KEY}
    try:
        res = requests.get(f"{DOCUSEAL_BASE}/submissions/{sub_id}", headers=headers)
        if not res.ok: return {"error": True, "message": "DocuSeal Record not found"}
        
        data = res.json()
        docs = data.get("documents", [])
        download_url = docs[0].get("url") if docs else None
        
        if data.get("status") == "completed" and download_url:
            file_res = requests.get(download_url)
            if file_res.ok:
                raw_name = data.get("name", "")
                
                # 1. 解析 Form 文件夹
                form_folder = "Others"
                if "Form A" in raw_name: form_folder = "Form A"
                elif "Form D" in raw_name: form_folder = "Form D"
                elif "Form 1" in raw_name: form_folder = "Form 1"

                # 2. 解析 Category 文件夹
                cat_folder = "Internal" if "[Internal]" in raw_name else "External"

                filename = f"{datetime.now().strftime('%Y%m%d')}_{raw_name}.pdf"
                safe_name = "".join([c for c in filename if c.isalnum() or c in (" ", "_", ".", "-")]).strip()
                
                # 终极路径结构: FormBucket/Internal/Form A/20260421_Internal_Project_Form_A.pdf
                final_path = f"{cat_folder}/{form_folder}/{safe_name}"
                
                supabase_admin.storage.from_("Form").upload(
                    path=final_path,
                    file=file_res.content,
                    file_options={"content-type": "application/pdf", "x-upsert": "true"}
                )
        return {"download_url": download_url}
    except Exception as e:
        return {"error": True, "message": str(e)}

@app.get("/api/get-signing-submissions")
async def get_submissions(user: dict = Depends(get_current_user)):
    headers = {"X-Auth-Token": DOCUSEAL_API_KEY}
    response = requests.get(f"{DOCUSEAL_BASE}/submissions", headers=headers)
    return response.json() if response.ok else []

@app.get("/api/get-document-download/{sub_id}")
async def get_download(sub_id: str, user: dict = Depends(get_current_user)):
    headers = {"X-Auth-Token": DOCUSEAL_API_KEY}
    try:
        res = requests.get(f"{DOCUSEAL_BASE}/submissions/{sub_id}", headers=headers)
        if not res.ok: return {"error": True, "message": "DocuSeal Record not found"}
        
        data = res.json()
        docs = data.get("documents", [])
        download_url = docs[0].get("url") if docs else None
        
        if data.get("status") == "completed" and download_url:
            file_res = requests.get(download_url)
            if file_res.ok:
                raw_name = data.get("name", "")
                # Logic to determine folder within Bucket 'Form'
                folder = "Others"
                if "Form A" in raw_name: folder = "Form A"
                elif "Form D" in raw_name: folder = "Form D"
                elif "Form 1" in raw_name: folder = "Form 1"

                filename = f"{datetime.now().strftime('%Y%m%d')}_{raw_name}.pdf"
                safe_name = "".join([c for c in filename if c.isalnum() or c in (" ", "_", ".", "-")]).strip()
                
                # Upload to Bucket 'Form' inside corresponding folder
                supabase_admin.storage.from_("Form").upload(
                    path=f"{folder}/{safe_name}",
                    file=file_res.content,
                    file_options={"content-type": "application/pdf", "x-upsert": "true"}
                )
        return {"download_url": download_url}
    except Exception as e:
        return {"error": True, "message": str(e)}
# ==========================================
# 4. Billing 逻辑 (保持上传版本)
# ==========================================
# 此处保留你原有的 process_pdf 逻辑入口...
# ==========================================
# 5. Billing 结算核心处理 API (维持原样)
# ==========================================
async def load_dataframe(file: UploadFile, sheet_name=None):
    if not file: raise ValueError("File is missing!")
    name = file.filename.lower()
    if name.endswith('.csv'): return pd.read_csv(file.file)
    if sheet_name:
        try: return pd.read_excel(file.file, sheet_name=sheet_name)
        except Exception:
            file.file.seek(0)
            return pd.read_excel(file.file)
    return pd.read_excel(file.file)

@app.post("/process-pdf")
async def process_pdf(files: List[UploadFile] = File(...), user: dict = Depends(get_current_user)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm, rate_file = None, None, None, None, None
        
        for f in files:
            name = f.filename.lower()
            if 'threshold' in name or 'rate' in name: rate_file = f
            elif ('gp' in name or 'goparkin' in name) and ('vehicle' in name or 'crm' in name): gp_crm = f
            elif ('sp' in name or 'evone' in name) and ('vehicle' in name or 'crm' in name): sp_crm = f
            elif ('gp' in name or 'goparkin' in name) and ('transaction' in name or 'row' in name): gp_tx = f
            elif ('sp' in name or 'evone' in name) and ('transaction' in name or 'report' in name or 'breakdown' in name): sp_tx = f

        missing = []
        if not gp_tx: missing.append("GoParkin Transaction")
        if not gp_crm: missing.append("GoParkin CRM")
        if not sp_tx: missing.append("SP Transaction")
        if not sp_crm: missing.append("SP CRM")
        if not rate_file: missing.append("Threshold and Rate")
        if missing: return {"error": True, "message": f"缺少文件: {', '.join(missing)}"}

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')
        df_rates = await load_dataframe(rate_file)
        
        rates_dict = {}
        for _, row in df_rates.iterrows():
            comp_name = str(row.get('company', '')).strip().lower()
            rates_dict[comp_name] = {
                'base': pd.to_numeric(row.get('base', 0), errors='coerce'),
                'threshold': pd.to_numeric(row.get('Threshold', 0), errors='coerce'),
                'discounted': pd.to_numeric(row.get('discounted', 0), errors='coerce')
            }

        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        
        if 'payment_status' in df_gp.columns: df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
        if 'transaction_type' in df_gp.columns: df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')

        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')

        def extract_details(df, source):
            res = pd.DataFrame()
            if df.empty: return res
            res['Company'] = df['Company']
            if source == 'GP':
                res['Vehicle_Email'] = df['vehicle_plate_number']
                res['Start Time'] = df.get('start_date_time', df['end_date_time'])
                res['End Time'] = df['end_date_time']
                res['Location'] = df.get('carpark_code', df.get('site_name', 'GoParkin Station'))
                res['Energy (kWh)'] = df['total_energy_supplied_kwh']
            else:
                res['Vehicle_Email'] = df['Driver Email']
                res['Start Time'] = df.get('Start Date', df.get('Date', ''))
                res['End Time'] = df.get('End Date', df.get('Date', ''))
                res['Location'] = df.get('Location Name', df.get('Location', 'SP Station'))
                res['Energy (kWh)'] = df['CDR Total Energy']
            return res

        all_details = pd.concat([extract_details(gp_merged, 'GP'), extract_details(sp_merged, 'SP')], ignore_index=True)
        all_details = all_details[all_details['Energy (kWh)'] > 0]
        all_details['Year-Month'] = all_details['End Time'].astype(str).str[0:7]

        zip_buffer = io.BytesIO()
        internal_summary_data = [] 

        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            months = all_details['Year-Month'].dropna().unique()
            for month in months:
                month_df = all_details[all_details['Year-Month'] == month]
                unique_companies = month_df['Company'].dropna().unique()
                used_file_names = set()
                
                for company in unique_companies:
                    comp_df = month_df[month_df['Company'] == company]
                    total_kwh = comp_df['Energy (kWh)'].sum()
                    comp_key = str(company).strip().lower()
                    r_info = rates_dict.get(comp_key, {'base': 0, 'threshold': float('inf'), 'discounted': 0})
                    
                    base_rate = r_info['base'] if pd.notna(r_info['base']) else 0
                    threshold = r_info['threshold'] if pd.notna(r_info['threshold']) else float('inf')
                    discounted = r_info['discounted'] if pd.notna(r_info['discounted']) else 0
                    applied_rate = discounted if total_kwh > threshold else base_rate
                    total_price = total_kwh * applied_rate

                    internal_summary_data.append({
                        "Billing Month": month,
                        "Company": company,
                        "Total Energy (kWh)": round(total_kwh, 2),
                        "Base Rate ($)": base_rate,
                        "Threshold (kWh)": threshold if threshold != float('inf') else "N/A",
                        "Discounted Rate ($)": discounted,
                        "Applied Rate ($)": applied_rate,
                        "Total Amount ($)": round(total_price, 2)
                    })

                    pdf_buf = io.BytesIO()
                    doc = SimpleDocTemplate(pdf_buf, pagesize=A4)
                    elements, styles = [], getSampleStyleSheet()
                    
                    logo_path = "static/logo.png"
                    if os.path.exists(logo_path):
                        logo_img = Image(logo_path, width=120, height=40) 
                        logo_img.hAlign = 'LEFT'
                        elements.extend([logo_img, Spacer(1, 10)])

                    elements.extend([
                        Paragraph(f"<b>Corporate Charging Statement</b>", styles['Title']), Spacer(1, 12),
                        Paragraph(f"<b>Company:</b> {company}", styles['Normal']),
                        Paragraph(f"<b>Billing Month:</b> {month}", styles['Normal'])
                    ])
                    disp_thresh = f"{threshold:g}" if threshold != float('inf') else "N/A"
                    elements.extend([
                        Paragraph(f"<b>Threshold Limit:</b> {disp_thresh}", styles['Normal']),
                        Paragraph(f"<b>Base Rate:</b> ${base_rate:.2f}", styles['Normal']),
                        Paragraph(f"<b>Discounted Rate:</b> ${discounted:.2f}", styles['Normal']),
                        Paragraph(f"<b>Applied Rate:</b> ${applied_rate:.2f}", styles['Normal']), Spacer(1, 20)
                    ])
                    
                    elements.append(Paragraph("<b>1. Billing Summary</b>", styles['Heading2']))
                    t_summary = Table([
                        ["Total Energy (kWh)", "Threshold Limit", "Applied Rate ($)", "Total Amount ($)"],
                        [f"{total_kwh:.2f}", f"{disp_thresh}", f"${applied_rate:.2f}", f"${total_price:.2f}"]
                    ], colWidths=[120, 110, 110, 120])
                    t_summary.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke), 
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0,0), (-1,0), 10),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.extend([t_summary, Spacer(1, 24)])
                    
                    elements.append(Paragraph("<b>2. Vehicle Breakdown</b>", styles['Heading2']))
                    veh_summary = comp_df.groupby('Vehicle_Email')['Energy (kWh)'].sum().reset_index().sort_values('Energy (kWh)', ascending=False)
                    veh_data = [["Vehicle / Driver Email", "Energy Used (kWh)"]]
                    for _, v_row in veh_summary.iterrows(): veh_data.append([str(v_row['Vehicle_Email']), f"{v_row['Energy (kWh)']:.2f}"])
                    t_veh = Table(veh_data, colWidths=[250, 150])
                    t_veh.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke), 
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.extend([t_veh, Spacer(1, 24)])

                    elements.extend([Paragraph("<b>3. Detailed Charging Log</b>", styles['Heading2']), Spacer(1, 10)])
                    for vehicle, grp in comp_df.groupby('Vehicle_Email'):
                        elements.extend([Paragraph(f"<b>Vehicle / Driver Email:</b> {vehicle}", styles['Normal']), Spacer(1, 6)])
                        detail_data = [["Location", "Start Time", "End Time", "Energy (kWh)"]]
                        veh_total = 0
                        for _, d_row in grp.sort_values('Start Time').iterrows():
                            detail_data.append([str(d_row['Location']), str(d_row['Start Time']), str(d_row['End Time']), f"{d_row['Energy (kWh)']:.2f}"])
                            veh_total += d_row['Energy (kWh)']
                        detail_data.append(["", "", "Total:", f"{veh_total:.2f}"])
                        t_detail = Table(detail_data, colWidths=[170, 100, 100, 80])
                        t_detail.setStyle(TableStyle([
                            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                            ('FONTNAME', (2,-1), (2,-1), 'Helvetica-Bold'), 
                            ('FONTNAME', (3,-1), (3,-1), 'Helvetica-Bold'), 
                            ('BACKGROUND', (0,-1), (-1,-1), colors.whitesmoke), 
                        ]))
                        elements.extend([t_detail, Spacer(1, 16)])

                    doc.build(elements)
                    
                    base_name = str(company).replace('/', '-').replace('\\', '-').replace(':', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', '').strip()
                    safe_comp = base_name
                    counter = 1
                    while safe_comp.lower() in used_file_names:
                        safe_comp = f"{base_name}_{counter}"
                        counter += 1
                    used_file_names.add(safe_comp.lower())

                    zip_file.writestr(f"{month}/{safe_comp}_{month}.pdf", pdf_buf.getvalue())

            if internal_summary_data:
                summary_df = pd.DataFrame(internal_summary_data)
                summary_df = summary_df.sort_values(by=['Billing Month', 'Total Amount ($)'], ascending=[True, False])
                
                excel_buf = io.BytesIO()
                with pd.ExcelWriter(excel_buf, engine='xlsxwriter') as writer:
                    summary_df.to_excel(writer, index=False, sheet_name='Internal Summary')
                
                zip_file.writestr("Internal_Summary_内部结算总表.xlsx", excel_buf.getvalue())

        del df_gp, df_sp, gp_merged, sp_merged, all_details, df_rates
        gc.collect()

        return Response(content=zip_buffer.getvalue(), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=Monthly_PDF_Reports.zip"})
    except Exception as e:
        return {"error": True, "message": str(e)}

@app.get("/analytics")
async def serve_analytics(request: Request):
    return templates.TemplateResponse(request=request, name="analytics.html")