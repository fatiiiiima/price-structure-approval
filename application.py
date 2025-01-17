from flask import Flask, request, render_template, redirect, url_for, jsonify, session
import pyodbc
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import bcrypt
import os
from dotenv import load_dotenv
import bcrypt
import pandas as pd
from flask import send_file
import io
import datetime
from pdf import generate_price_structure_pdf

load_dotenv()


app = Flask(__name__)

# Database connection details
server = 'Factory'
database = 'TestDB'

app.secret_key = os.getenv('SECRET_KEY')
conn_str = os.getenv('DB_CONN_STR')


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# conn_str = f"""
#     DRIVER={{SQL Server}};
#     SERVER={server};
#     DATABASE={database};
#     Trusted_Connection=yes;
# """



print(conn_str)

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role

# Flask-Login user loader
@login_manager.user_loader
def load_user(user_id):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = "SELECT id, username, role FROM users WHERE id = ?"
        cursor.execute(query, user_id)
        row = cursor.fetchone()
        if row:
            return User(row[0], row[1], row[2])
    except Exception as e:
        print("Error loading user:", e)
    return None

# Route: Login Page
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        try:
            # Connect to the database
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Check the user in the database
            query = "SELECT id, username, password_hash, role FROM users WHERE username = ?"
            cursor.execute(query, username)
            row = cursor.fetchone()

            if row:
                user_id, db_username, db_password_hash, role = row

                # Verify the password
                if bcrypt.checkpw(password.encode('utf-8'), db_password_hash.encode('utf-8')):
                    # Login the user
                    user = User(user_id, db_username, role)
                    login_user(user)
                    return redirect(url_for("dashboard"))

            return "Invalid username or password", 401
        except Exception as e:
            print("Database error:", e)
            return "An error occurred", 500

    return render_template("login.html")  # Renders the login page

# Route: Dashboard Page
@app.route("/dashboard")
@login_required
def dashboard():
    if current_user.role == 'ttsapprover':  # Check if the user is 'alsafadi'
        return render_template('financepage.html')  # Render the specific page
    
    if current_user.role == 'cogsapprover':  # Check if the user is 'alsafadi'
        return render_template('cogspage.html')  # Render the specific page

    if current_user.role == 'marketing':  # Check if the user's role is 'marketing'
        return render_template('formupdate.html')

    if current_user.role == 'admin':
        return render_template('adminpage.html')
    
    if current_user.role == 'manager':
        return render_template('managerpage.html')
    
    if current_user.role == 'cdmanager':
        return render_template('cdmanagerpage.html')
    
    # Default dashboard view for other roles
    return f"Welcome, {current_user.username}! Your role is: {current_user.role}. <a href='/logout'>Logout</a>"


# Route: Logout
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# Route: Test Page
@app.route("/")
def index():
    return redirect(url_for("login"))


def get_sku_data(sku_number, country):
    """
    Fetch SKU data from the Master1 table based on SKU number and country.
    Also fetch additional data from Qatar_PS, SKU_tts, and Sheet1$ tables.
    """
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # First query: Fetch from Master1
        query = """
        SELECT 
            [SKU Description], Brand, Sector, Flavor, Format, Packing, Project, Type, [VAT%]
      ,[RM %]
      ,[WSM %]
      ,[DM %]
      ,[Duty %]
      ,[Clearing Charges %]
      ,[BD %]
      ,[CPP%]
        FROM SKU_tts
        WHERE [SKU Code] = ? AND Country = ?
        """
        cursor.execute(query, sku_number, country)
        row = cursor.fetchone()

        # Initialize the result dictionary
        result = {}
        if row:
            result = {
                "description": row[0],
                "brand": row[1],
                "sector": row[2],
                "flavor": row[3],
                "format": row[4],
                "packing": row[5],
                "project": row[6],
                "type": row[7],
                "vat": row[8],
                "rm": row[9],
                "wsm": row[10],
                "dm": row[11],
                "duty": row[12],
                "clearingcharges": row[13],
                "bd": row[14],
                "cpp": row[15],
            }

        #Additional query: Fetch TTS% from SKU_tts table
        query_tts = """
        SELECT [TTS%]
        FROM SKU_tts
        WHERE [SKU Code] = ? AND [Country] = ?

        """
        cursor.execute(query_tts, sku_number, country)
        row_tts = cursor.fetchone()
        if row_tts:
            result["tts"] = row_tts[0]
            result["tts"] = f"{round(float(result['tts']) * 100, 2)}%"

        # # Additional query: Fetch Total COGS from Sheet1$ table
        query_cogs = """
        SELECT [Total COGS]
        FROM [Sheet1$]
        WHERE [SKU Code] = ?

        """
        cursor.execute(query_cogs, sku_number)
        row_cogs = cursor.fetchone()
        if row_cogs:
            total_cogs_usd = round(row_cogs[0])
            result["total_cogs_usd"] = round(row_cogs[0])
            
        query_psc_per_case = """
        SELECT [PCS_per_Case]
        FROM [SKU_Master$ExternalData_2]
        WHERE [Unique_SKU_Code] = ? 
        """
        cursor.execute(query_psc_per_case, sku_number)
        row_case = cursor.fetchone()
        if row_case:
            #case per ton not pcs per case cogs per case / case per ton
            pcs_per_case = row_case[0]
            result["pcs_per_case"] = row_case[0]
        # Additional query for Qatar-specific data

        query_case_per_ton = """
        SELECT [Case_per_Ton]
        FROM [SKU_Master$ExternalData_2]
        WHERE [Unique_SKU_Code] = ? 
        """
        
        cursor.execute(query_case_per_ton, sku_number)
        row_per_ton = cursor.fetchone()
        if row_per_ton:
            case_per_ton = row_per_ton[0]
            print(case_per_ton)
            result["case_per_ton"] = row_per_ton[0]
        
        
        query_rate = """
        SELECT [toUSD]
        FROM CurrencyRates
        WHERE Country = ?
        """
        cursor.execute(query_rate, country)
        
        row_currency = cursor.fetchone()
        if row_currency:
            currency_rate = row_currency[0]
            result["currency_rate"] = row_currency[0]
            
            if total_cogs_usd and currency_rate:
                total_cogs_local = round(total_cogs_usd * currency_rate)  # Convert to local currency and round
                result["total_cogs_local"] = total_cogs_local
                
                if case_per_ton and total_cogs_local:
                    cogs_per_case = round(total_cogs_local / case_per_ton)
                    result["cogs_per_case"] = cogs_per_case
                
        
        if country == 'Qatar':
            query_qatar = """
            SELECT 
                [Proposed RSP (inc VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM Qatar_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_qatar, sku_number)
            row_qatar = cursor.fetchone()
            if row_qatar:
                result.update({
                    "rsp": round(row_qatar[0], 2),
                    "bptt": round(row_qatar[1],2),
                    "cif": round(row_qatar[2],2),
                })
        
        elif country == 'Kuwait':
            query_kuwait = """
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM Kuwait_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_kuwait, sku_number)
            row_kuwait = cursor.fetchone()
            if row_kuwait:
                result.update({
                    "rsp": round(row_kuwait[0],2),
                    "bptt": round(row_kuwait[1], 2),
                    "cif": round(row_kuwait[2]),
                })

        elif country == 'Oman':
            query_oman = """
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM Oman_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_oman, sku_number)
            row_oman = cursor.fetchone()
            if row_oman:
                result.update({
                    "rsp": round(row_oman[0],2),
                    "bptt": round(row_oman[1],2),
                    "cif": round(row_oman[2],2),
                })

        elif country == 'Bahrain':
            query_bahrain = """
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM Bahrain_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_bahrain, sku_number)
            row_bahrain = cursor.fetchone()
            if row_bahrain:
                result.update({
                    "rsp": round(row_bahrain[0],2),
                    "bptt": round(row_bahrain[1],2),
                    "cif": round(row_bahrain[2],2),
                })

        elif country == 'KSA':
            query_ksa = """
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM KSA_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_ksa, sku_number)
            row_ksa = cursor.fetchone()
            if row_ksa:
                result.update({
                    "rsp": round(row_ksa[0],2),
                    "bptt": round(row_ksa[1]),
                    "cif": round(row_ksa[2]),
                })

        elif country == 'UAE':
            query_uae = """
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case]
            FROM UAE_PS_New
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_uae, sku_number)
            row_uae = cursor.fetchone()
            if row_uae:
                result.update({
                    "rsp": round(row_uae[0],2),
                    "bptt": round(row_uae[1],2),
                    "cif": round(row_uae[2],2),
                })
                
                
                    
            

        # Close the connection
        conn.close()
        
        
        

        return result if result else None
    except Exception as e:
        print("Database error:", e)
        return None

def hash_password(password):
    # Generate salt and hash
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


@app.route("/update")
def next_page():
    return render_template('formupdate.html')


@app.route("/get_skus", methods=["GET"])
def get_skus():
    try:
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Query the database for distinct SKU values
        cursor.execute("SELECT DISTINCT [SKU Code] FROM SKU_tts")  # Adjust table/column names if needed
        skus = [row[0] for row in cursor.fetchall()]  # Fetch all SKUs into a list

        # Close the connection
        conn.close()

        return jsonify({"skus": skus}), 200
    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Failed to fetch SKUs"}), 500

@app.route("/get_sku_info", methods=["POST"])
def get_sku_info():
    data = request.json
    sku_number = data.get("sku_number")
    country = data.get("country")

    # Fetch SKU information from database
    sku_info = get_sku_data(sku_number, country)
    if sku_info:
        sku_info["country"] = country  # Add country for context
        return jsonify(sku_info), 200
    else:
        return jsonify({"error": f"SKU {sku_number} does not exist for {country}"}), 404

@app.route('/calculate_results', methods=['POST'])
def calculate_results():
    try:
        # Parse incoming JSON request
        data = request.get_json()

        # Extract values from the request
        new_rsp = float(data.get('newRSP', 0))
        new_vat = float(data.get('newVat', 0))
        new_rm = float(data.get('newRM', 0))
        new_wsm = float(data.get('newWSM', 0))
        new_dm = float(data.get('newDM', 0))
        new_duty = float(data.get('newDuty', 0))
        new_cc = float(data.get('newCC', 0))
        new_bd = float(data.get('newBD', 0))
        new_cpp = float(data.get('newCPP', 0))
        print("this is the cpp", new_cpp)
        pcs = float(data.get('pcs'))
        new_tts_percentage = float(data.get('newTTS', 0))
        new_tts = new_tts_percentage / 100
        print("this is the new tts percentage", new_tts_percentage)
        print("this is the new tts percentage", new_tts)

        print("this is the pieces per case", pcs)
        print("this is the dm",new_dm)
        rsp_without_vat = new_rsp / (1 + new_vat)
        print("this is rsp without vat",rsp_without_vat)
        rsp_per_case = rsp_without_vat * pcs
        print("this is rsp per case",rsp_per_case)
        retail_markup = rsp_per_case / (1 + new_rm)
        print("this is retail markuo",retail_markup)
        bptt = retail_markup / (1 + new_wsm)
        print("this is the new bptt",bptt)
        dplc = bptt / (1 + new_dm)
        print("this is dplc",dplc)
        cif = (dplc / (1 + new_duty + new_cc)) - new_bd
        print("this is the new cif",cif)
        
        
        
        gsv = cif / (1 + new_cpp)
        
        print("this is the gsv", gsv)
        tts = gsv * new_tts
        print("this is the tts", tts)
        to = gsv - tts
        print("this is the to", to)
        cogs_per_case = float(data.get("cogs_local_per_case", 0))
        print("this is the cogs", cogs_per_case)
        gp = to - cogs_per_case
        
        print("this is the gp", gp)
        
        
        gm = gp / to
        
        print("this is the gross margin", gm)

        print(bptt)
        print(cif)
        return jsonify({
            "newBPTT": bptt,
            "newCIF":cif,
            "newRSP": new_rsp, 
            "newGSV":gsv, 
            "tts":tts, 
            "to":to,
            "gp":gp, 
            "gm":gm, 
            "tts_percentage": new_tts_percentage,
            "cogs":cogs_per_case, 
            
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/submit_request', methods=['POST'])
@login_required
def submit_request():
    if current_user.role != 'marketing':
        return "Unauthorized", 403

    data = request.get_json()

    sku_code = data['sku_code']
    country = data['country']
    rsp = data['new_rsp']
    tts_percentage = data['new_tts']
    bptt_new = data['bptt']
    cif_new = data["cif"]
    gsv_new = data["gsv"]
    to_new = data["to"]
    gp_new = data["gp"]
    gm_new = data["gm"]
    cogs_new = data["cogs"]

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch approvers
    cursor.execute("SELECT id, username FROM users WHERE role = 'ttsapprover'")
    finance = cursor.fetchone()

    cursor.execute("SELECT id, username FROM users WHERE role = 'cogsapprover'")
    cogs_approver = cursor.fetchone()
    
    if not finance or not cogs_approver:
        return "Approvers not found", 404
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id = f"{sku_code}_{country}_{year_month}"  # Combine into the required format

    # Check if a request with the same SKU code and country exists
    cursor.execute("""SELECT id, status FROM ApprovalRequestsWithDetails 
                      WHERE sku_code = ? AND country = ? AND status != 'Request Approved'""",
                   sku_code, country)
    existing_request = cursor.fetchone()

    if existing_request:
        existing_id, existing_status = existing_request
        # Update the status of the existing request to INACTIVE
        cursor.execute("""UPDATE ApprovalRequestsWithDetails 
                          SET status = 'INACTIVE', current_approver_id = NULL 
                          WHERE id = ?""", existing_id)

    # Insert the new request for TTS approval
    cursor.execute("""INSERT INTO ApprovalRequestsWithDetails 
                      (sku_code, country, requester_id, current_approver_id, approver_name, rsp, 
                       tts_percentage, status, bptt, cif, gsv, too, gp, gm, cogs, 
                       requester_name, approval_type, next_approver_id, request_id) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                   sku_code, country, current_user.id, finance[0], finance[1], rsp, tts_percentage,
                   'Pending', bptt_new, cif_new, gsv_new, to_new, gp_new, gm_new, cogs_new,
                   current_user.username, 'TTS', cogs_approver[0], request_id)

    conn.commit()
    conn.close()

    return jsonify({"message": "Request submitted successfully"}), 201



@app.route('/approve_tts', methods=['POST'])
@login_required
def approve_tts():
    if current_user.role != 'ttsapprover':
        return "Unauthorized", 403

    data = request.get_json()
    request_id = data['request_id']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    # Approve TTS and update the SKU_tts table and assign the approval to the next person aka cogs approver
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails 
        SET status = 'TTS Approved', updated_at = GETDATE(), approval_type = 'COGS', 
        next_approver_id = (
            SELECT id FROM users WHERE role = 'manager'
            ), 
        current_approver_id = (
            SELECT id FROM users WHERE role = 'cogsapprover'
        ),
        approver_name = (
            SELECT name FROM users WHERE role = 'ttsapprover'
        ), 
        request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'TTS'
    """, request_id_code, request_id, current_user.id)

    # cursor.execute("""
    #     SELECT sku_code, tts_percentage 
    #     FROM ApprovalRequestsWithDetails 
    #     WHERE id = ?
    # """, request_id)
    # request_details = cursor.fetchone()

    # if request_details:
    #     sku_code, tts_percentage = request_details
    #     tts_percentage = tts_percentage / 100
    #     cursor.execute("""
    #         UPDATE SKU_tts 
    #         SET [TTS%] = ?
    #         WHERE [SKU Code] = ?
    #     """, tts_percentage, sku_code)

    conn.commit()
    conn.close()

    return jsonify({"message": "TTS approved and updated successfully"}), 200

@app.route('/reject_tts', methods=['POST'])
@login_required
def reject_tts():
    if current_user.role != 'ttsapprover':
        return "Unauthorized", 403

    data = request.get_json()
    request_id = data['request_id']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    

    # Reject TTS request and update fields accordingly
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails 
        SET status = 'TTS Rejected', 
            updated_at = GETDATE(), 
            approval_type = 'Rejected', 
            next_approver_id = NULL,
            current_approver_id = NULL, 
            approver_name = (
                SELECT name FROM users WHERE role = 'ttsapprover'
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'TTS'
    """, request_id_code, request_id, current_user.id)

    conn.commit()
    conn.close()

    return jsonify({"message": "TTS request rejected successfully."}), 200


@app.route('/approve_cogs', methods=['POST'])
@login_required
def approve_cogs():
    if current_user.role != 'cogsapprover':
        return "Unauthorized", 403

    data = request.get_json()
    print("COGS data:", data)
    request_id = data['request_id']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    

    # Get the country for the request
    cursor.execute("""
        SELECT country 
        FROM ApprovalRequestsWithDetails 
        WHERE id = ?
    """, request_id)
    country_row = cursor.fetchone()

    if not country_row:
        return jsonify({"error": "Request not found"}), 404

    country = country_row[0]
    
    cursor.execute("""
        SELECT CD_Manager
        FROM CountryDetails
        WHERE Country = ?""", 
        country)
    cd_manager_name = cursor.fetchone()
    
    print(cd_manager_name[0])
    
    # Get the CD Manager ID for the country
    cursor.execute("""
        SELECT id 
        FROM users 
        WHERE role = 'cdmanager' AND name = ?
    """, cd_manager_name[0])
    cd_manager_row = cursor.fetchone()

    
    if not cd_manager_row:
        return jsonify({"error": f"No CD manager found for country {country}"}), 400

    cd_manager_id = cd_manager_row[0]

    # Approve COGS and set the next approver
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails 
        SET status = 'COGS Approved', approval_type = 'Approval',
            updated_at = GETDATE(), 
            current_approver_id = (
             SELECT id FROM users WHERE role = 'manager'
            ), 
            next_approver_id = ?, 
            approver_name = (
                SELECT name FROM users WHERE role = 'cogsapprover'
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'COGS'
    """, cd_manager_id, request_id_code, request_id, current_user.id)

    conn.commit()
    conn.close()

    return jsonify({"message": "COGS approved successfully"}), 200

@app.route('/reject_cogs', methods=['POST'])
@login_required
def reject_cogs():
    if current_user.role != 'cogsapprover':
        return "Unauthorized", 403

    data = request.get_json()
    request_id = data['request_id']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check if the request exists
    cursor.execute("""
        SELECT id 
        FROM ApprovalRequestsWithDetails 
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'COGS'
    """, request_id, current_user.id)
    request_row = cursor.fetchone()

    if not request_row:
        return jsonify({"error": "Request not found or unauthorized"}), 404


    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    # Reject COGS request and update the fields
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails 
        SET status = 'COGS Rejected', 
            updated_at = GETDATE(), 
            approval_type = 'Rejected',
            next_approver_id = NULL,
            current_approver_id = NULL, 
            approver_name = (
                SELECT name FROM users WHERE role = 'cogsapprover'
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'COGS'
    """,request_id_code,  request_id, current_user.id)

    conn.commit()
    conn.close()

    return jsonify({"message": "COGS request rejected successfully."}), 200



@app.route('/approve_request', methods=['POST'])
@login_required
def approve_request():
    if current_user.role not in ['finance', 'marketing', 'admin']:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.get_json()
    request_id = data['request_id']
    decision = data['decision']  # "approve" or "reject"

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch the current request
    cursor.execute("SELECT * FROM ApprovalRequests WHERE id = ?", request_id)
    request_data = cursor.fetchone()

    if not request_data:
        return jsonify({"error": "Request not found"}), 404

    next_approver_id = None

    if decision == 'approve':
        # Determine the next approver
        if current_user.role == 'finance':
            cursor.execute("SELECT id FROM users WHERE role = 'marketing'")
            next_approver_id = cursor.fetchone()[0]
        elif current_user.role == 'marketing':
            cursor.execute("SELECT id FROM users WHERE role = 'commercial_finance'")
            next_approver_id = cursor.fetchone()[0]
        elif current_user.role == 'commercial_finance':
            cursor.execute("SELECT id FROM users WHERE role = 'country_manager'")
            next_approver_id = cursor.fetchone()[0]
        elif current_user.role == 'country_manager':
            cursor.execute("SELECT id FROM users WHERE username = 'Sudha'")
            next_approver_id = cursor.fetchone()[0]

        if next_approver_id:
            # Assign the request to the next approver
            cursor.execute("""
                UPDATE ApprovalRequests
                SET current_approver_id = ?, updated_at = GETDATE()
                WHERE id = ?
            """, next_approver_id, request_id)
        else:
            # Fully approve the request
            cursor.execute("""
                UPDATE ApprovalRequests
                SET status = 'Approved', updated_at = GETDATE()
                WHERE id = ?
            """, request_id)
    elif decision == 'reject':
        # Reject the request and return it to the requester
        cursor.execute("""
            UPDATE ApprovalRequests
            SET current_approver_id = requester_id, status = 'Rejected', updated_at = GETDATE()
            WHERE id = ?
        """, request_id)

    conn.commit()
    conn.close()

    return jsonify({"message": "Request updated successfully"}), 200



@app.route('/pending_requests', methods=['GET'])
@login_required
def pending_requests():
    print(f"Current approver ID: {current_user.id}")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM ApprovalRequestsWithDetails WHERE current_approver_id = ? ORDER BY created_at DESC
    """, current_user.id)
    requests = cursor.fetchall()
    print(requests)
    conn.close()

    response_data = [dict(zip([column[0] for column in cursor.description], row)) for row in requests]
    print(f"Response to frontend: {response_data}")
    return jsonify(response_data), 200


# get all requests for the admin user
@app.route("/all_requests", methods=["GET"])
@login_required
def all_requests():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # fetching all requests, including their status
    cursor.execute("""
        SELECT 
            request_id, sku_code, country, requester_id, current_approver_id, approver_name, 
            rsp, tts_percentage, status, bptt, cif, gsv, too, gp, gm, cogs, 
            requester_name, created_at, updated_at, approval_type, id
        FROM ApprovalRequestsWithDetails 
        WHERE status != 'INACTIVE'
        ORDER BY created_at DESC
    """)

    requests = cursor.fetchall()
    conn.close()
    
    # transforming data into a list of dictionaries for easier rendering in the template
    requests_list = [
        {
            "id": row[0],
            "sku_code": row[1],
            "country": row[2],
            "requester_id": row[3],
            "current_approver_id": row[4],
            "approver_name": row[5],
            "rsp": row[6],
            "tts_percentage": row[7],
            "status": row[8],
            "bptt": row[9],
            "cif": row[10],
            "gsv": row[11],
            "too": row[12],
            "gp": row[13],
            "gm": row[14] * 100,
            "cogs": row[15],
            "requester_name": row[16],
            "created_at": row[17],
            "updated_at": row[18], 
            "approval_type": row[19],
            "unique_id": row[20]
        } for row in requests
    ]

    print(requests_list)
    return jsonify(requests_list), 200



@app.route("/export_requests", methods=["GET"])
@login_required
def export_requests():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # fetching all requests
    cursor.execute("""
        SELECT 
            sku_code, country, approver_name, 
            rsp, tts_percentage, status, gm, 
            requester_name, created_at, updated_at
        FROM ApprovalRequestsWithDetails
        ORDER BY created_at DESC
    """)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()

    # Convert data to a Pandas DataFrame
    df = pd.DataFrame.from_records(data, columns=columns)

    # Write the DataFrame to an Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        df.to_excel(writer, index=False, sheet_name='Requests')
    output.seek(0)

    # Return the file to the user
    return send_file(
        output,
        as_attachment=True,
        download_name='requests.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )



@app.route("/new_user", methods=["POST"])
@login_required
def create_new_user():
    if current_user.role != 'admin':
        return jsonify({"error": "Unauthorized"}), 403

    # Parse JSON data
    data = request.json
    if not data:
        return jsonify({"error": "Invalid input"}), 400

    name = data.get("name")
    username = data.get("username")
    password = data.get("password")
    role = data.get("role")

    # Validate required fields
    if not all([name, username, password, role]):
        return jsonify({"error": "Missing required fields"}), 400

    # Define roles that require uniqueness
    unique_roles = [
        "finance", "cogsapprover", "qatarcdmanager", "kuwaitcdmanager",
        "uaecdmanager", "bahraincdmanager", "ksacdmanager", "omancdmanager"
        "financemanager", "ttsapprover"
    ]

    # Hash the password
    password_hash = hash_password(password)

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check if the username already exists
        cursor.execute("SELECT COUNT(*) FROM users WHERE username = ?", username)
        if cursor.fetchone()[0] > 0:
            return jsonify({"error": "Username already exists"}), 400

        # Check if the role requires uniqueness and already exists
        if role in unique_roles:
            cursor.execute("SELECT COUNT(*) FROM users WHERE role = ?", role)
            if cursor.fetchone()[0] > 0:
                return jsonify({"error": f"A user with the role '{role}' already exists"}), 400

        # Insert the new user into the database
        cursor.execute("""
            INSERT INTO users (name, username, password_hash, role)
            VALUES (?, ?, ?, ?)
        """, (name, username, password_hash, role))

        conn.commit()
        conn.close()

        return jsonify({"message": "User created successfully"}), 201

    except Exception as e:
        print("Error creating user:", e)
        return jsonify({"error": "Failed to create user"}), 500


    
@app.route("/approved_requests", methods=["GET"])
@login_required
def approved_requests():
    if current_user.role != 'manager':
        return "Unauthorized", 403

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # fetching all requests, including their status
    cursor.execute("""
        SELECT 
            id, sku_code, country, requester_id, current_approver_id, approver_name, 
            rsp, tts_percentage, status, bptt, cif, gsv, too, gp, gm, cogs, 
            requester_name, created_at, updated_at
        FROM ApprovalRequestsWithDetails
        WHERE status = 'COGS Approved'
        ORDER BY created_at DESC
    """)

    approved_requests = cursor.fetchall()
    conn.close()
    
    # transforming data into a list of dictionaries for easier rendering in the template
    approved_requests_list = [
        {
            "id": row[0],
            "sku_code": row[1],
            "country": row[2],
            "requester_id": row[3],
            "current_approver_id": row[4],
            "approver_name": row[5],
            "rsp": row[6],
            "tts_percentage": row[7],
            "status": row[8],
            "bptt": row[9],
            "cif": row[10],
            "gsv": row[11],
            "too": row[12],
            "gp": row[13],
            "gm": row[14],
            "cogs": row[15],
            "requester_name": row[16],
            "created_at": row[17],
            "updated_at": row[18]
        } for row in approved_requests
    ]

    print("approved:",approved_requests_list)
    return jsonify(approved_requests_list), 200


@app.route("/approve_pre_final", methods=["POST"])
@login_required
def approve_pre_final():
    if current_user.role != 'manager':
        return "Unauthorized", 403
    
    data = request.get_json()
    request_id = data['request_id']
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails
        SET status = 'Approved', approval_type = 'FinalApproval',
            updated_at = GETDATE(),
            current_approver_id = next_approver_id, 
            next_approver_id = null, 
            approver_name = (
                SELECT name FROM users WHERE role = 'manager'
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'Approval'
    """, request_id_code, request_id, current_user.id)
    
    conn.commit()
    conn.close()
    
    return jsonify({"message": "Approval approved successfully!"}), 200

@app.route("/reject_pre_final", methods=["POST"])
@login_required
def reject_pre_final():
    if current_user.role != 'manager':
        return "Unauthorized", 403

    data = request.get_json()
    request_id = data['request_id']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
   
    # Validate the request
    cursor.execute("""
        SELECT id 
        FROM ApprovalRequestsWithDetails 
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'Final Approval'
    """, request_id, current_user.id)
    request_row = cursor.fetchone()

    if not request_row:
        return jsonify({"error": "Request not found or unauthorized"}), 404

    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    # Update the request to rejected
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails
        SET status = 'Rejected', 
            updated_at = GETDATE(),
            current_approver_id = NULL, 
            next_approver_id = NULL, 
            approver_name = (
                SELECT name FROM users WHERE role = 'manager'
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'Final Approval'
    """, request_id_code, request_id, current_user.id)

    conn.commit()
    conn.close()

    return jsonify({"message": "Request rejected successfully!"}), 200

    
@app.route("/approve_final", methods=["POST"])
@login_required
def final_approval():
    if current_user.role != 'cdmanager':
        return "Unauthorized", 403
    
    data = request.get_json()
    request_id = data['request_id']
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails
        SET status = 'Request Approved', approval_type = 'Approved',
            updated_at = GETDATE(),
            current_approver_id = null, 
            next_approver_id = null, 
            approver_name = (
                SELECT name FROM users WHERE id = ?
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'FinalApproval'
    """, current_user.id, request_id_code, request_id, current_user.id)
    
    cursor.execute("""
        SELECT country, sku_code, tts_percentage, bptt, cif 
        FROM ApprovalRequestsWithDetails 
        WHERE id = ?
    """, request_id)
    request_details = cursor.fetchone()
    
    if request_details:
        country, sku_code, tts_percentage, bptt, cif = request_details
        tts_percentage = tts_percentage / 100
        cursor.execute("""
            UPDATE SKU_tts 
            SET [TTS%] = ?
            WHERE [SKU Code] = ?
        """, tts_percentage, sku_code)
        
        if country == 'Qatar':
            cursor.execute("""
            UPDATE [Qatar_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
        
        elif country == 'Kuwait':
            cursor.execute("""
            UPDATE [Kuwait_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
            
        elif country == 'KSA':
            cursor.execute("""
            UPDATE [KSA_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
            
        elif country == 'UAE':
            cursor.execute("""
            UPDATE [UAE_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
        
        elif country == 'Oman':
            cursor.execute("""
            UPDATE [Oman_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
            
        elif country == 'Bahrain':
            cursor.execute("""
            UPDATE [Bahrain_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, sku_code)
            
        
        
    
    
    conn.commit()
    conn.close()
    
    return jsonify({"message": "Request approved and TTS, BPTT, and CIF uploaded successfully!"}), 200


@app.route("/reject_final", methods=["POST"])
@login_required
def final_reject():
    if current_user.role != 'cdmanager':
        return "Unauthorized", 403
    
    data = request.get_json()
    request_id = data['request_id']
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country FROM ApprovalRequestsWithDetails WHERE id = ?""", 
        request_id)

    data = cursor.fetchone()
    print(data)
    
    updated_at = datetime.datetime.now()  # Current timestamp
    year_month = updated_at.strftime("%Y-%m")  # Format as 'year-month'
    request_id_code = f"{data[0]}_{data[1]}_{year_month}"
    
    
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails
        SET status = 'Request Rejected', approval_type = 'Rejected',
            updated_at = GETDATE(),
            current_approver_id = null, 
            next_approver_id = null, 
            approver_name = (
                SELECT name FROM users WHERE id = ?
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'FinalApproval'
    """,  current_user.id, request_id_code, request_id, current_user.id)
    
    
    conn.commit()
    conn.close()
    
    return jsonify({"message": "Request rejected succesfully!"}), 200



@app.route("/export_sap_template", methods=["GET"])
@login_required
def export_sap_template():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    unique_id = request.args.get("unique_id")
    if not unique_id:
        return jsonify({"error": "Missing unique_id parameter"}), 400

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch data for the specific request
    cursor.execute("""
        SELECT sku_code, country, cif, updated_at
        FROM ApprovalRequestsWithDetails
        WHERE id = ?
    """, (unique_id,))
    data = cursor.fetchone()

    if not data:
        return jsonify({"error": "No data found for the given unique_id"}), 404

    sku_code, country, cif, updated_at = data
    

    if country == 'Qatar':
        currency = 'qar'
        query_qatar = """
        SELECT [Pack_Type]
        FROM SKU_Master$ExternalData_2
        WHERE [Material_Code] = ?
        """
        cursor.execute(query_qatar, (sku_code,))
        enitity_qatar = cursor.fetchone()
        
        query_currency = """
        SELECT ToUSD 
        FROM CurrencyRates
        WHERE Country = ?
        """
        cursor.execute(query_currency, (country,))
        currency_value = cursor.fetchone()
        
        cif = round(float(cif) * float(currency_value[0]), 2)
            
        if enitity_qatar:
            if enitity_qatar[0] =='Tea Bags':
                sales_organization = 5800
            else:
                sales_organization = 3330
            # result["Sales Organization"] = sales_organization
            # result["Document Currency"] = 'qar'
            # result["Dest. Country/Region"] = country
            # result["Condition Currency"] = 'qar'
            
    elif country in ['KSA', 'Oman', 'Bahrain']:
        sales_organization = 3330
        currency = 'usd'
        
    elif country == 'UAE':
        sales_organization = 3300
        currency = 'usd'  # CIF stays the same in USD
        
    elif country == 'Kuwait':
        currency = 'kwd'
        query_kuwait = """
        SELECT ToUSD 
        FROM CurrencyRates
        WHERE Country = 'Kuwait'
        """
        cursor.execute(query_kuwait)
        currency_value_kuwait = cursor.fetchone()
        cif = round(float(cif) * float(currency_value_kuwait[0]), 2)
        
        sales_organization = 3330
    
    
           
        
    
    result = {
        "Condition Type": 'Z000',
        "Condition Table": '932',
        "Access":country,
        "Sales Organization":sales_organization,
        "Distribution Channel": '10',
        "Product": sku_code,
        "Release Status": '',
        "Document Currency":currency,
        "Departure Ctry/Reg.": '',
        "Dest. Country/Region":country,
        "Condition Record No.": '$$00000001',
        "Sequent.No. of Cond.": '01',
        "Is Scale": '',
        "Char":"",
        "Valid From": updated_at,
        "Valid To": '99991231',
        "Condition Amount or Ratio": cif,
        "Condition Currency": currency,
        "Pricing Unit":"",
        "Unit of Measure":"",
        "Lower Limit":"",
        "Upper Limit":"",
        "Condition Description":"",
        "Terms of Payment":"",
        "Fixed Value Date":"",
        "Addit. Value Days":"",
        "Calculation Type":"",
        "Scale Type":"",
        "Scale Quantity":"",
        "Scale Unit of Meas.":"",
        "Scale Value":"",
        "Scale Currency":"",
        "Amount":"", 
    }
    conn.close()

    # Create a DataFrame from the result
    df = pd.DataFrame([result])

    # Write the DataFrame to an Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='SAP Template')
    output.seek(0)

    # Serve the file to the frontend
    return send_file(
        output,
        as_attachment=True,
        download_name=f'sap_template_{unique_id}.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    
@app.route("/export_pdf_file", methods=["GET"])
@login_required
def export_pdf_file():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    unique_id = request.args.get("unique_id")
    if not unique_id:
        return jsonify({"error": "Missing unique_id parameter"}), 400
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("""
            SELECT sku_code, country, updated_at, rsp, cif, bptt
            FROM ApprovalRequestsWithDetails
            WHERE id = ?
    """, unique_id)
    
    result = cursor.fetchone()
    
    if not result:
        return jsonify({"error": "No data found for the given unique_id"}), 404
    
    sku_code, country, updated_At, rsp, cif, bptt = result
    
    if country == 'Qatar':
        cursor.execute("""
        SELECT [Enitity], [SKU Description], [DB SKU], [VAT %], [VAT], [RM %], [RSP/Cs_LC], [Retail Markup LC] ,[Retail Price LC],[WSM %], [W/Sale Markup LC], [DM %] ,[Distributor Markup LC],[DPLC LC/case]
        ,[Duty %]
        ,[Duty]
        ,[Clearing Charges %]
        ,[Clearing Charges]
        FROM Qatar_PS_New
        WHERE [SKU Code] = ?
        """, sku_code)
    
    field_values = cursor.fetchone()
    
    entity, desc, db_sku, vat_per, vat, rm_per, rsp_lc, retail_markup_lc, retail_price, wsm_per, w_sale, dm_per, distributor_markup, dplc, duty_per, duty, cc_per, cc = field_values

    # Format percentage values as strings with percentage sign
    vat_per = f"{vat_per * 100:.0f}%" if vat_per is not None else None
    rm_per = f"{rm_per * 100:.0f}%" if rm_per is not None else None
    wsm_per = f"{wsm_per * 100:.0f}%" if wsm_per is not None else None
    dm_per = f"{dm_per * 100:.0f}%" if dm_per is not None else None
    duty_per = f"{duty_per * 100:.0f}%" if duty_per is not None else None
    cc_per = f"{cc_per * 100:.0f}%" if cc_per is not None else None

    # Round numerical values with decimals to two places
    variables = [rsp_lc, rm_per, retail_markup_lc, retail_price, wsm_per, w_sale,
                 dm_per, distributor_markup, dplc, duty_per, duty, cc_per, cc]

# Replace None values with 0 or another default value before rounding
    rounded_values = [
    v if isinstance(v, str) or v is None else round(v, 2) for v in variables
        ]

    rsp_lc, rm_per, retail_markup_lc, retail_price, wsm_per, w_sale, dm_per, \
    distributor_markup, dplc, duty_per, duty, cc_per, cc = rounded_values
    
    cursor.execute("""
                 SELECT name FROM users WHERE role = 'manager'
        """)
    finance_manager = cursor.fetchone()
    
    formatted_date = updated_At.strftime("%Y-%m-%d")
    
    if country == 'Qatar':
        currency = 'QAR'
        
    elif country in ['KSA', 'Oman', 'Bahrain', 'UAE']:
        currency = 'USD'
        
    elif country == 'Kuwait':
        currency = 'KWD'
    
    entity_int = int(entity)
    entity_string = str(entity_int)
    
    full_string = entity_string + currency
    full_string2 = entity_string + "Ekaterra Gulf FZE (Free zone entity)"
    
    data = {
        "date_of_communication": formatted_date, 
        "distributor": "",
        "country": country, 
        "effective_date_from": formatted_date,
        "effective_date_till": "Till next communication of Price update", 
        "invoicing_currency": full_string  , 
        "invoicing_entity": full_string2,
        "finance_member": "Kunal Thakwani",
        "table_data": [
            [
                entity, 
                sku_code,
                db_sku, 
                desc,
                0, 
                vat_per, 
                vat, 
                rsp,
                rsp_lc,
                rm_per, 
                retail_markup_lc, 
                retail_price, 
                wsm_per,
                w_sale, 
                bptt, 
                dm_per, 
                distributor_markup, 
                dplc, 
                duty_per,
                duty, 
                cc_per, 
                cc, 
                cif
            ]
        ]
    }
    
    output_file = generate_price_structure_pdf(data) 
    
    return send_file(output_file, as_attachment=True)


   
    
from flask import request, jsonify
import bcrypt

@app.route("/forgot_password", methods=['POST'])
@login_required
def forgot_password():
    if current_user.role != 'admin':
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    username = data.get('username')
    new_password = data.get('new_password')  # Updated to match the frontend key

    if not username or not new_password:
        return jsonify({"error": "Username and new password are required"}), 400

    # Hash the new password
    hashed_password = hash_password(new_password)

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    update_query = """
        UPDATE [TestDB].[dbo].[users]
        SET password_hash = ?
        WHERE username = ?
    """
    
    cursor.execute(update_query, (hashed_password, username))
    conn.commit()

    return jsonify({"message": f"Password for {username} has been updated successfully."})

@app.route("/delete_user", methods=["POST"])
@login_required
def delete_user():
    if current_user.role != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    username = data.get("username")

    if not username:
        return jsonify({"error": "Username is required"}), 400

    try:
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check if the user exists
        cursor.execute("SELECT role FROM dbo.users WHERE username = ?", username)
        user = cursor.fetchone()

        if not user:
            return jsonify({"error": "User not found"}), 404

        # Update the user's role to NULL
        cursor.execute("UPDATE dbo.users SET username = 'null', role = 'Deleted' WHERE username = ?", username)
        conn.commit()

        return jsonify({"message": f"User '{username}' role has been deleted."}), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred while deleting the user"}), 500

    finally:
        cursor.close()
        conn.close()

@app.route('/updatetables')
@login_required
def update_tables():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    return render_template('updatetables.html')  

@app.route('/backadmindasboard')
def back_admin_dashboard():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    return render_template('adminpage.html')


@app.route('/view_requests') 
def view_requests():
    return render_template('marketingrequests.html')

@app.route('/all_marketing_requests')
@login_required
def all_marketing_requests():
    # Ensure only 'marketing' users can access this route
    if current_user.role != 'marketing':
        return "Unauthorized", 403

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        # Query to fetch requests made by the current user
        cursor.execute("""
            SELECT id, sku_code, country, status, created_at, rsp, tts_percentage, cogs, gm, gp, approval_type, requester_name
            FROM ApprovalRequestsWithDetails
            WHERE requester_id = ? AND status != 'INACTIVE'
        """, current_user.id)

        # Fetch all rows from the query
        requests = cursor.fetchall()

        # Convert rows into a list of dictionaries
        requests_list = [
            {
                "id": row[0],
                "sku_code": row[1],
                "country": row[2],
                "status": row[3],
                "created_at": row[4].strftime("%Y-%m-%d %H:%M:%S"),  # Format datetime
                "rsp": row[5],
                "tts_percentage": row[6],
                "cogs": row[7],
                "gm": row[8],
                "gp": row[9],
                "approval_type": row[10],
                "requester_name": row[11],
            }
            for row in requests
        ]

        # Return the requests as JSON
        return jsonify(requests_list), 200

    except Exception as e:
        print("Error fetching marketing requests:", e)
        return jsonify({"error": "Failed to fetch requests"}), 500

    finally:
        conn.close()

    
    
    

@app.route('/update_currency', methods=['POST'])
def update_currency():
    data = request.json
    country = data.get("country")
    toUSD = data.get("toUSD")
    
    if not country or not toUSD:
        return jsonify({"error": "Country and ToUSD rate are required."}), 400
    
    try:
        # Update the database here
        # Example: 
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Update CurrencyRates SET ToUSD = ? WHERE Country = ?
        # db.execute("UPDATE CurrencyRates SET ToUSD = ? WHERE Country = ?", (toUSD, country))
        # db.commit()
        cursor.execute("UPDATE CurrencyRates SET ToUSD = ? WHERE Country = ?", (toUSD, country))
        conn.commit()

        return jsonify({"message": f"Currency rate for {country} updated to {toUSD}."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/upload_excel', methods=['POST'])
@login_required
def upload_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)

        # Ensure the required columns are present
        required_columns = [
            'Country', 'SKU Code', 'SKU Description', 'Brand', 'Sector',
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'VAT%',
            'RM %', 'WSM %', 'DM %', 'Duty %', 'Clearing Charges %', 'BD %',
            'CPP%', 'TTS%'
        ]
        
        if not all(column in df.columns for column in required_columns):
            return jsonify({"error": "Excel file is missing required columns."}), 400

        # Replace NaN with default values (e.g., 0 for floats)
        # df = df.fillna({
        #     'Duty %': 0.0,
        #     'CPP%': 0.0
        # })

        # Limit precision for numeric fields
        numeric_columns = [
            'VAT%', 'RM %', 'WSM %', 'DM %', 'Duty %',
            'Clearing Charges %', 'BD %', 'CPP%', 'TTS%'
        ]
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(float)

        # Convert categorical and string fields
        categorical_columns = ['Country', 'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 'Project', 'Type']
        for column in categorical_columns:
            df[column] = df[column].astype(str)

        # Connect to SQL Server
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear the existing table
        cursor.execute("TRUNCATE TABLE [SKU_tts]")

        # Insert the new data
        for _, row in df.iterrows():
            cursor.execute(
                    """
                    INSERT INTO [SKU_tts] (
                        Country, [SKU Code], [SKU Description], Brand, Sector, Flavor,
                        Format, Packing, Project, Type, [VAT%], [RM %], [WSM %], [DM %],
                        [Duty %], [Clearing Charges %], [BD %], [CPP%], [TTS%]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row['Country'], row['SKU Code'], row['SKU Description'], row['Brand'],
                    row['Sector'], row['Flavor'], row['Format'], row['Packing'], row['Project'],
                    row['Type'], float(row['VAT%']), float(row['RM %']), float(row['WSM %']),
                    float(row['DM %']), float(row['Duty %']), float(row['Clearing Charges %']),
                    float(row['BD %']), float(row['CPP%']), float(row['TTS%'])
        )

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/upload_cogs_excel', methods=['POST'])
@login_required
def upload_cogs_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    
    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'Country','SKU Code', 'SKU Descr', 'RM', 'PM', 'BI', 'RM/PM/BI', 'Production cost', 
            'Distribution cost', 'BW', 'Total COGS'
        ]
        
        if not all(column in df.columns for column in required_columns):
            return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'RM', 'PM', 'BI', 'RM/PM/BI', 'Production cost', 
            'Distribution cost', 'BW', 'Total COGS'
        ]
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(float)

        categorical_columns = ['Country','SKU Code', 'SKU Descr']
        for column in categorical_columns:
            df[column] = df[column].astype(str)
    
    
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE [Sheet1$]")
        
        for _, row in df.iterrows():
            cursor.execute(
                    """
                    INSERT INTO [Sheet1$] (
                        Country, [SKU Code], [SKU Descr], RM, PM, BI, [RM/PM/BI], [Production cost], [Distribution cost], [BW], [Total COGS]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row['Country'], row['SKU Code'], row['SKU Descr'], float(row['RM']),
                    float(row['PM']), float(row['BI']), float(row['RM/PM/BI']), 
                    float(row['Production cost']), float(row['Distribution cost']), 
                    float(row['BW']), float(row['Total COGS'])
        )
            
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/upload_master_excel', methods=['POST'])
@login_required
def upload_master_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403 

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        # Ensure all required columns are present
        required_columns = [
            'Material_Code', 'SKU_Description', 'Packsize', 'SKU_Type', 'VCP', 
            'Case_per_Pallet', 'Case_per_Ton', 'PCS_per_Case', 'Unit_Weight', 
            'Unique_SKU_Code', 'Unique_SKU_Description', 'Unique_SKU_Desc', 
            'Packing Type', 'Procurement Type', 'Format', 'Remarks', 'Global_Mapping',
            'CPG', 'S4Hana_Code', 'S4Hana_PH', 'Other Groupings', 'Packsize_Description',
            'Subbrand_Description', 'CPG_Description Original', 'Sector_Description', 
            'Brand_Description', 'Market_Description', 'Category_Description',
            'Sub_Division', 'Division', 'Packing', 'Brand_Group', 'CPG_Description',
            'Pack_Type', 'Tea_Type', 'Hero SKU-UAE', 'Hero SKU-KSA', 'Brand Group New',
            'Sector / Format', 'CPG Code'
        ]
        
        if not all(column in df.columns for column in required_columns):
            return jsonify({"error": "Excel file is missing required columns."}), 400

        # Convert specific numeric columns
        numeric_columns = [ 'Case_per_Pallet','Case_per_Ton', 'PCS_per_Case', 'Unit_Weight', 'Unique_SKU_Code', 'Format','CPG', 'S4Hana_Code','CPG Code' ]
        
        
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).round(3).astype(float)
            print(df[column])



        # Convert categorical columns to string
        categorical_columns = [
            'Material_Code', 'SKU_Description', 'Packsize','SKU_Type', 'VCP','Unique_SKU_Description','Unique_SKU_Desc','Packing Type', 
            'Procurement Type', 'Remarks', 'Global_Mapping',  
            'S4Hana_PH', 'Other Groupings', 'Packsize_Description', 
            'Subbrand_Description', 'CPG_Description Original', 'Sector_Description', 
            'Brand_Description', 'Market_Description', 'Category_Description', 
            'Sub_Division', 'Division', 'Packing', 'Brand_Group', 'CPG_Description', 
            'Pack_Type', 'Tea_Type', 'Hero SKU-UAE', 'Hero SKU-KSA', 
            'Brand Group New', 'Sector / Format'
        ]
        for column in categorical_columns:
            df[column] = df[column].astype(str).replace('nan', '').replace('NaN', '')
            
        print(df[numeric_columns].info())
        print(df[numeric_columns].head())

        # Connect to SQL Server
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [SKU_Master$ExternalData_2]")

        # Insert new data
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO [SKU_Master$ExternalData_2] (
                        Material_Code, SKU_Description, Packsize, SKU_Type, VCP, 
                        Case_per_Pallet, Case_per_Ton, PCS_per_Case, Unit_Weight, 
                        Unique_SKU_Code, Unique_SKU_Description, Unique_SKU_Desc, 
                        [Packing Type], [Procurement Type], Format, Remarks, Global_Mapping,
                        CPG, S4Hana_Code, S4Hana_PH, [Other Groupings], Packsize_Description,
                        Subbrand_Description, [CPG_Description Original], Sector_Description, 
                        Brand_Description, Market_Description, Category_Description,
                        Sub_Division, Division, Packing, Brand_Group, CPG_Description,
                        Pack_Type, Tea_Type, [Hero SKU-UAE], [Hero SKU-KSA], [Brand Group New],
                        [Sector / Format], [CPG Code]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row['Material_Code'], row['SKU_Description'], row['Packsize'], row['SKU_Type'], row['VCP'], 
                    float(row['Case_per_Pallet']), float(row['Case_per_Ton']), float(row['PCS_per_Case']), float(row['Unit_Weight']), 
                    float(row['Unique_SKU_Code']), row['Unique_SKU_Description'], row['Unique_SKU_Desc'], 
                    row['Packing Type'], row['Procurement Type'], float(row['Format']), row['Remarks'], row['Global_Mapping'],
                    float(row['CPG']), float(row['S4Hana_Code']), row['S4Hana_PH'], row['Other Groupings'], row['Packsize_Description'],
                    row['Subbrand_Description'], row['CPG_Description Original'], row['Sector_Description'], 
                    row['Brand_Description'], row['Market_Description'], row['Category_Description'], 
                    row['Sub_Division'], row['Division'], row['Packing'], row['Brand_Group'], row['CPG_Description'], 
                    row['Pack_Type'], row['Tea_Type'], row['Hero SKU-UAE'], row['Hero SKU-KSA'], 
                    row['Brand Group New'], row['Sector / Format'], float(row['CPG Code'])
                )
            except Exception as e:
                print(f"Error with row {idx}: {row.to_dict()}")
                print(f"Exception: {e}")
                break


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

import pandas as pd
from sqlalchemy import create_engine

# @app.route('/upload_qatar_ps_excel', methods=['POST'])
# @login_required
# def upload_qatar_ps_excel():
#     if current_user.role != 'admin':
#         return "Unauthorized", 403

#     try:
#         # Check if a file was uploaded
#         if 'file' not in request.files:
#             return jsonify({"error": "No file provided"}), 400

#         file = request.files['file']

#         # Check if the file is an Excel file
#         if not file.filename.endswith(('.xls', '.xlsx')):
#             return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

#         # Read the Excel file into a pandas DataFrame
#         df = pd.read_excel(file)
        
#         required_columns = [
#             'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
#             'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
#             'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
#             'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
#             'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
#             'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
#             'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
#             'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
#             'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
#             'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
#             'Z000'
#         ]
        
#         column_rename_mapping = {
#             "RSP/Cs\nLC": "RSP/Cs_LC",
#             # Add additional mappings if column names in Excel differ from table columns
#         }
#         df.rename(columns=column_rename_mapping, inplace=True)

#         # Rename unnamed columns to match the SQL table schema
#         unnamed_columns = df.filter(like='Unnamed').columns.tolist()
#         print(unnamed_columns)
#         column_mapping = {
#             unnamed_columns[0]: 'F43',
#             unnamed_columns[1]: 'F46',
#             unnamed_columns[2]: 'F47'
#         }
#         df.rename(columns=column_mapping, inplace=True)
#         print(df.columns)
        
        
        
#         # if not all(column in df.columns for column in required_columns):
            
#         #     return jsonify({"error": "Excel file is missing required columns."}), 400

#         numeric_columns = [
#             'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
#             'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
#             'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
#             'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
#             'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
#             'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
#             'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
#             'BPTT  LC/Piece', 'Check'
#         ]
        
#         for column in numeric_columns:
#             if column in df.columns:
#                 df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


#         categorical_columns = [
#             'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
#             'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU', 'Valid to'
#         ]
#         for column in categorical_columns:
#             if column in df.columns:
#                 df[column] = df[column].astype(str).fillna('')
                
            
#         if 'Valid from' in df.columns:
#             df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()

#         # Clear existing table
#         cursor.execute("TRUNCATE TABLE [Qatar_PS_New]") 
        
#         for idx, row in df.iterrows():
#             try:
#                 cursor.execute(
#                 """
#                 INSERT INTO [Qatar_PS_New] (
#                     [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], 
#                     [SKU Description], [Brand], [Sector], [Flavor], [Format], [Packing], 
#                     [Project], [Type], [SU], [Cases/Ton], [Units/Cs], [Valid from], [Valid to],
#                     [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC],
#                     [RSP/Cs_LC], [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], 
#                     [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
#                     [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges], 
#                     [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [F43], [Stock], 
#                     [Z521 SAP], [F46], [F47], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], 
#                     [CIF $/Ton], [GSV/Ton $], [BPTT  LC/Piece], [Check], [Z009], [Z521], 
#                     [Z000]
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
#                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
#                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 """,
#                 (
#                     row['DD code'], float(row['Enitity']), float(row['SKU Code']),
#                     row['Comments'], float(row['DB SKU']), row['CPD code'], 
#                     row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
#                     row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
#                     float(row['Cases/Ton']), float(row['Units/Cs']), row['Valid from'], 
#                     row['Valid to'], float(row['Proposed RSP (inc VAT) LC']), float(row['VAT %']),
#                     float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
#                     float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
#                     float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
#                     float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
#                     float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
#                     float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
#                     float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
#                     float(row['GSV LC/case']), float(row['F43']), float(row['Stock']), 
#                     row['Z521 SAP'], float(row['F46']), float(row['F47']), 
#                     float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
#                     float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
#                     float(row['Check']), row['Z009'], row['Z521'], row['Z000']
#                 )
#             )
#             except Exception as e:
#                 print(f"Error at row {idx}: {e}")
#                 print(f"Column causing error: {column}")
#                 print(f"Value: {row[column]}")
#                 print("Parameters:")


#         conn.commit()
#         cursor.close()
#         conn.close()

#         return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# @app.route('/upload_qatar_ps_excel', methods=['POST'])
# @login_required
# def upload_qatar_ps_excel():
#     if current_user.role != 'admin':
#         return "Unauthorized", 403

#     try:
#         # Check if a file was uploaded
#         if 'file' not in request.files:
#             return jsonify({"error": "No file provided"}), 400

#         file = request.files['file']

#         # Check if the file is an Excel file
#         if not file.filename.endswith(('.xls', '.xlsx')):
#             return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

#         # Read the Excel file into a pandas DataFrame
#         df = pd.read_excel(file)
        
#         required_columns = [
#             'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
#             'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
#             'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
#             'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
#             'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
#             'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
#             'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
#             'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
#             'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
#             'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
#             'Z000'
#         ]
        
#         column_rename_mapping = {
#             "RSP/Cs\nLC": "RSP/Cs_LC",
#             # Add additional mappings if column names in Excel differ from table columns
#         }
#         df.rename(columns=column_rename_mapping, inplace=True)

#         # Rename unnamed columns to match the SQL table schema
#         unnamed_columns = df.filter(like='Unnamed').columns.tolist()
#         print(unnamed_columns)
#         column_mapping = {
#             unnamed_columns[0]: 'F43',
#             unnamed_columns[1]: 'F46',
#             unnamed_columns[2]: 'F47'
#         }
#         df.rename(columns=column_mapping, inplace=True)
#         print(df.columns)
        
        
        
#         # if not all(column in df.columns for column in required_columns):
            
#         #     return jsonify({"error": "Excel file is missing required columns."}), 400

#         numeric_columns = [
#             'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
#             'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
#             'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
#             'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
#             'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
#             'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
#             'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
#             'BPTT  LC/Piece', 'Check'
#         ]
        
#         for column in numeric_columns:
#             if column in df.columns:
#                 df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


#         categorical_columns = [
#             'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
#             'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
#         ]
#         for column in categorical_columns:
#             if column in df.columns:
#                 df[column] = df[column].astype(str).fillna('')
                
            
#         if 'Valid from' in df.columns:
#             df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()

#         # Clear existing table
#         cursor.execute("TRUNCATE TABLE [Qatar_PS_New]") 
        
#         for idx, row in df.iterrows():
#             try:
#                 cursor.execute(
#                 """
#                 INSERT INTO [Qatar_PS_New] (
#                     [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
#                     [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
#                     [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
#                     [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
#                     [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
#                     [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT  LC/Piece], [Check], [Z009], [Z521],                      [Z000]
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 """,
#                 (
#                     row['DD code'], float(row['Enitity']), float(row['SKU Code']),
#                     row['Comments'], float(row['DB SKU']), row['CPD code'], 
#                     row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
#                     row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
#                     float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
#                     float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
#                     float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
#                     float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
#                     float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
#                     float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
#                     float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
#                     float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
#                     float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
#                     float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
#                     float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
#                 )
#             )
#             except Exception as e:
#                 print(f"Error at row {idx}: {e}")
#                 print(f"Column causing error: {column}")
#                 print(f"Value: {row[column]}")
#                 print("Parameters:")


#         conn.commit()
#         cursor.close()
#         conn.close()

#         return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

@app.route('/upload_ksa_ps_excel', methods=['POST'])
@login_required
def upload_ksa_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT  LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [KSA_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [KSA_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/upload_qatar_ps_excel', methods=['POST'])
@login_required
def upload_qatar_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT  LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [Qatar_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [Qatar_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload_bahrain_ps_excel', methods=['POST'])
@login_required
def upload_bahrain_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [Bahrain_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [Bahrain_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    
@app.route('/upload_uae_ps_excel', methods=['POST'])
@login_required
def upload_uae_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT  LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [UAE_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [UAE_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload_kuwait_ps_excel', methods=['POST'])
@login_required
def upload_kuwait_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT  LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [Kuwait_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [Kuwait_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload_oman_ps_excel', methods=['POST'])
@login_required
def upload_oman_ps_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403

    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'DD code', 'Enitity', 'SKU Code', 'Comments', 'DB SKU', 'CPD code', 
            'SKU Description', 'Brand', 'Sector', 'Flavor', 'Format', 'Packing', 
            'Project', 'Type', 'SU', 'Cases/Ton', 'Units/Cs', 'Valid from', 'Valid to',
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'Stock', 
            'Z521 SAP', 'F46', 'F47', 'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 
            'CIF $/Ton', 'GSV/Ton $', 'BPTT  LC/Piece', 'Check', 'Z009', 'Z521', 
            'Z000'
        ]
        
        column_rename_mapping = {
            "RSP/Cs\nLC": "RSP/Cs_LC",
            # Add additional mappings if column names in Excel differ from table columns
        }
        df.rename(columns=column_rename_mapping, inplace=True)

        # Rename unnamed columns to match the SQL table schema
        unnamed_columns = df.filter(like='Unnamed').columns.tolist()
        print(unnamed_columns)
        column_mapping = {
            unnamed_columns[0]: 'F43',
            unnamed_columns[1]: 'F46',
            unnamed_columns[2]: 'F47'
        }
        df.rename(columns=column_mapping, inplace=True)
        print(df.columns)
        
        
        
        # if not all(column in df.columns for column in required_columns):
            
        #     return jsonify({"error": "Excel file is missing required columns."}), 400

        numeric_columns = [
            'Enitity', 'SKU Code', 'DB SKU', 'Cases/Ton', 'Units/Cs', 
            'Proposed RSP (inc VAT) LC', 'VAT %', 'VAT', 'Proposed RSP (ex VAT) LC',
            'RSP/Cs_LC', 'RM %', 'Retail Markup LC', 'Retail Price LC', 'WSM %', 
            'W/Sale Markup LC', 'BPTT LC/Case', 'DM %', 'Distributor Markup LC', 
            'DPLC LC/case', 'Duty %', 'Duty', 'Clearing Charges %', 'Clearing Charges', 
            'BD', 'CIF LC/case', 'CPP%', 'CPP', 'GSV LC/case', 'F43', 'F46', 'F47', 
            'BPTT $/Case', 'CIF $/Case', 'BPTT $/Ton', 'CIF $/Ton', 'GSV/Ton $', 
            'BPTT  LC/Piece', 'Check'
        ]
        
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)


        categorical_columns = [
            'DD code', 'Comments', 'CPD code', 'SKU Description', 'Brand', 'Sector', 
            'Flavor', 'Format', 'Packing', 'Project', 'Type', 'SU'
        ]
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).fillna('')
                
            
        if 'Valid from' in df.columns:
            df['Valid from'] = pd.to_datetime(df['Valid from'], errors='coerce').dt.strftime('%Y-%m-%d')

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Clear existing table
        cursor.execute("TRUNCATE TABLE [Oman_PS_New]") 
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(
                """
                INSERT INTO [Oman_PS_New] (
                    [DD code], [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code], [SKU Description], 
                    [Brand], [Sector], [Flavor], [Format], [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs], 
                    [Proposed RSP (inc VAT) LC], [VAT %], [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], 
                    [RM %], [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC], 
                    [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %], [Clearing Charges],
                    [BD], [CIF LC/case], [CPP%], [CPP], [GSV LC/case], [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton], [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521],                      [Z000]
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['DD code'], float(row['Enitity']), float(row['SKU Code']),
                    row['Comments'], float(row['DB SKU']), row['CPD code'], 
                    row['SKU Description'], row['Brand'], row['Sector'], row['Flavor'], 
                    row['Format'], row['Packing'], row['Project'], row['Type'], row['SU'], 
                    float(row['Cases/Ton']), float(row['Units/Cs']), float(row['Proposed RSP (inc VAT) LC']), 
                    float(row['VAT %']), float(row['VAT']), float(row['Proposed RSP (ex VAT) LC']), 
                    float(row['RSP/Cs_LC']), float(row['RM %']), float(row['Retail Markup LC']), 
                    float(row['Retail Price LC']), float(row['WSM %']), float(row['W/Sale Markup LC']),
                    float(row['BPTT LC/Case']), float(row['DM %']), float(row['Distributor Markup LC']),
                    float(row['DPLC LC/case']), float(row['Duty %']), float(row['Duty']), 
                    float(row['Clearing Charges %']), float(row['Clearing Charges']), row['BD'], 
                    float(row['CIF LC/case']), float(row['CPP%']), float(row['CPP']), 
                    float(row['GSV LC/case']), float(row['BPTT $/Case']), float(row['CIF $/Case']), float(row['BPTT $/Ton']), 
                    float(row['CIF $/Ton']), float(row['GSV/Ton $']), float(row['BPTT  LC/Piece']), 
                    float(row['Check']), row['Z009'], row['Z521'], row['Z000']
                    
                )
            )
            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Column causing error: {column}")
                print(f"Value: {row[column]}")
                print("Parameters:")


        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})

     
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/upload_cd_excel")
@login_required
def upload_cd_excel():
    if current_user.role != 'admin':
        return "Unauthorized", 403
    
    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']

        # Check if the file is an Excel file
        if not file.filename.endswith(('.xls', '.xlsx')):
            return jsonify({"error": "Invalid file format. Please upload an Excel file."}), 400

        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(file)
        
        required_columns = [
            'Country', 'VAT_Percentage', 'CD_Manager', 'DB_Manager'
        ]
        
        numeric_columns = [
            'VAT_Percentage'
        ]
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
            
        categorical_columns = ['Country', 'CD_Manager', 'DB_Manager']
        for column in categorical_columns:
            df[column] = df[column].astype(str)
        # Connect to SQL Server
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE [CountryDetails]")
        
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO [CountryDetails] (
                [Country], [VAT_Percentage], [CD_Manager], [DB_Manager]
                ) VALUES (?, ?, ?, ?)
                """, 
                row['Country'], float(row['VAT_Percentage']), row['CD_Manager'], row['DB_Manager']
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({"message": "Table successfully updated with the uploaded Excel file!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


        

if __name__ == "__main__":
    app.run(debug=True)
