from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from typing import List
import pandas as pd
import io
import warnings
import gc
import zipfile
import os
from dotenv import load_dotenv

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

warnings.filterwarnings('ignore')
load_dotenv()

app = FastAPI(title="EVOne Billing API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")

@app.get("/config")
async def get_config():
    return {
        "supabase_url": os.environ.get("SUPABASE_URL"),
        "supabase_key": os.environ.get("SUPABASE_ANON_KEY")
    }

security = HTTPBearer()
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalid or expired. 请重新登录。",
            headers={"WWW-Authenticate": "Bearer"},
        )

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
        internal_summary_data = [] # 👉 新增：用于收集内部总表数据的列表

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

                    # 收集该公司的核心数据，用于最后生成总表
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

            # 👉 新增：在压缩包里塞入“内部总表”
            if internal_summary_data:
                summary_df = pd.DataFrame(internal_summary_data)
                # 按照月份和金额排序，让老板看起来更直观
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