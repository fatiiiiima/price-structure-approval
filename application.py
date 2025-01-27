from flask import Flask, request, render_template, redirect, url_for, jsonify, session,  Response
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
    Fetch SKU data from the SKU_tts table based on SKU number and country.
    If the SKU doesn't exist for the given country, return a not-found message.
    """
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check if the SKU code exists for the given country
        check_query = """
        SELECT 1
        FROM SKU_tts
        WHERE [SKU Code] = ? AND Country = ?
        """
        cursor.execute(check_query, sku_number, country)
        sku_exists = cursor.fetchone()

        if not sku_exists:
            conn.close()
            return {"error": f"SKU Code {sku_number} not found for the country {country}"}

        # Fetch data from the SKU_tts table
        query = """
        SELECT 
            [SKU Description], Brand, Sector, Flavor, Format, Packing, Project, Type, [VAT%],
            [RM %], [WSM %], [DM %], [Duty %], [Clearing Charges %], [BD %], [CPP%]
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
                "vat": round(row[8] * 100, 2),
                "rm": round(row[9] * 100, 2),
                "wsm": round(row[10] * 100, 2),
                "dm": round(row[11] * 100, 2),
                "duty": round(row[12] * 100, 2),
                "clearingcharges": round(row[13] * 100, 2),
                "bd": round(row[14] * 100, 2),
                "cpp": round(row[15] * 100, 2),
            }

        # Fetch additional data (TTS%, Total COGS, PCS per case, Case per ton, etc.)
        query_tts = """
        SELECT [TTS%]
        FROM SKU_tts
        WHERE [SKU Code] = ? AND [Country] = ?
        """
        cursor.execute(query_tts, sku_number, country)
        row_tts = cursor.fetchone()
        if row_tts:
            result["tts"] = f"{round(float(row_tts[0]) * 100, 2)}%"

        query_cogs = """
        SELECT [Total COGS]
        FROM [Sheet1$]
        WHERE [SKU Code] = ?
        """
        cursor.execute(query_cogs, sku_number)
        row_cogs = cursor.fetchone()
        if row_cogs:
            total_cogs_usd = round(row_cogs[0])
            result["total_cogs_usd"] = total_cogs_usd

        query_psc_per_case = """
        SELECT [PCS_per_Case]
        FROM [SKU_Master$ExternalData_2]
        WHERE [Unique_SKU_Code] = ?
        """
        cursor.execute(query_psc_per_case, sku_number)
        row_case = cursor.fetchone()
        if row_case:
            pcs_per_case = row_case[0]
            result["pcs_per_case"] = pcs_per_case

        query_case_per_ton = """
        SELECT [Case_per_Ton]
        FROM [SKU_Master$ExternalData_2]
        WHERE [Unique_SKU_Code] = ?
        """
        cursor.execute(query_case_per_ton, sku_number)
        row_per_ton = cursor.fetchone()
        if row_per_ton:
            case_per_ton = round(row_per_ton[0], 0)
            result["case_per_ton"] = case_per_ton

        query_rate = """
        SELECT [toUSD]
        FROM CurrencyRates
        WHERE Country = ?
        """
        cursor.execute(query_rate, country)
        row_currency = cursor.fetchone()
        if row_currency:
            currency_rate = row_currency[0]
            result["currency_rate"] = currency_rate

            if total_cogs_usd and currency_rate:
                total_cogs_local = round(total_cogs_usd * currency_rate, 0)
                result["total_cogs_local"] = total_cogs_local

                if case_per_ton and total_cogs_local:
                    cogs_per_case = round(total_cogs_local / case_per_ton, 0)
                    result["cogs_per_case"] = cogs_per_case

        # Fetch country-specific data
        country_queries = {
            "Qatar": ("Qatar_PS_New", "QAR"),
            "Kuwait": ("Kuwait_PS_New", "KWD"),
            "Oman": ("Oman_PS_New", "OMR"),
            "Bahrain": ("Bahrain_PS_New", "BHD"),
            "KSA": ("KSA_PS_New", "SAR"),
            "UAE": ("UAE_PS_New", "AED"),
        }

        if country in country_queries:
            table_name, currency = country_queries[country]
            query_country = f"""
            SELECT 
                [Proposed RSP (ex VAT) LC], [BPTT LC/Case], [CIF LC/case], [RSP/Cs_LC]
            FROM {table_name}
            WHERE [SKU Code] = ?
            """
            cursor.execute(query_country, sku_number)
            row_country = cursor.fetchone()
            if row_country:
                result.update({
                    "rsp": round(row_country[0], 2),
                    "bptt": round(row_country[1], 2),
                    "cif": round(row_country[2], 2),
                    "rsppercase": round(row_country[3], 2),
                    "currency": currency,
                })

        # Close the connection
        conn.close()

        return result

    except Exception as e:
        print("Database error:", e)
        return {"error": "An error occurred while fetching SKU data"}

def hash_password(password):
    # Generate salt and hash
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


@app.route("/update")
def next_page():
    return render_template('formupdate.html')


# @app.route("/get_skus", methods=["GET"])
# def get_skus():
#     try:
#         # Connect to the database
#         conn = pyodbc.connect(conn_str)
#         cursor = conn.cursor()

#         # Query the database for distinct SKU values
#         cursor.execute("SELECT DISTINCT [SKU Code] FROM SKU_tts")  # Adjust table/column names if needed
#         skus = [row[0] for row in cursor.fetchall()]  # Fetch all SKUs into a list

#         # Close the connection
#         conn.close()

#         return jsonify({"skus": skus}), 200
#     except Exception as e:
#         print("Error:", e)
#         return jsonify({"error": "Failed to fetch SKUs"}), 500

@app.route("/get_sku_descriptions", methods=["GET"])
def get_sku_descriptions():
    try:
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Query the database for distinct SKU descriptions
        cursor.execute("SELECT DISTINCT [SKU Description] FROM SKU_tts")  # Adjust table/column names if needed
        descriptions = [row[0] for row in cursor.fetchall()]  # Fetch all descriptions into a list

        # Close the connection
        conn.close()

        return jsonify({"descriptions": descriptions}), 200
    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "Failed to fetch descriptions"}), 500


@app.route("/get_skus_by_description", methods=["POST"])
def get_skus_by_description():
    try:
        data = request.json
        description = data.get("description")

        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Query the database for SKUs matching the description
        cursor.execute(
            "SELECT DISTINCT [SKU Code] FROM SKU_tts WHERE [SKU Description] = ?", description
        )
        skus = [row[0] for row in cursor.fetchall()]  # Fetch matching SKUs into a list

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

    if not sku_number or not country:
        return jsonify({"error": "Both 'sku_number' and 'country' are required fields"}), 400

    # Fetch SKU information from the database
    sku_info = get_sku_data(sku_number, country)
    
    # Check for error in the result
    if "error" in sku_info:
        return jsonify({"error": sku_info["error"]}), 404

    # Add country for context
    sku_info["country"] = country
    return jsonify(sku_info), 200

    


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
        
        
        pcs = float(data.get('pcs'))
        new_tts_percentage = float(data.get('newTTS', 0))
        new_tts = new_tts_percentage / 100
      
        rsp_without_vat = new_rsp / (1 + new_vat)
        
        rsp_per_case = rsp_without_vat * pcs
      
        retail_markup = rsp_per_case / (1 + new_rm)
      
        bptt = retail_markup / (1 + new_wsm)
       
        dplc = bptt / (1 + new_dm)
        
        cif = (dplc / (1 + new_duty + new_cc)) - new_bd
        
        
        
        
        gsv = cif / (1 + new_cpp)
        
      
        tts = gsv * new_tts
        to = gsv - tts
        cogs_per_case = float(data.get("cogs_local_per_case", 0))
        gp = to - cogs_per_case
        
        
        
        gm = gp / to 
        gm_percentage = gm * 100

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
            "gm":gm_percentage, 
            "tts_percentage": new_tts_percentage,
            "cogs":cogs_per_case, 
            "vat":new_vat,
            "rm":new_rm,
            "wsm":new_wsm,
            "dm":new_dm,
            "clearingcharges":new_cc,
            "bd":new_bd,
            "cpp":new_cpp,
            "newRSPPerCase":rsp_per_case,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
    
# @app.route('/calculate_new_results', methods=['POST'])
# @login_required
# def calculate_new_results():
#   
    
@app.route('/calculate_new_results', methods=['POST'])
@login_required
def calculate_new_results():
    try:
        data = request.get_json()
        request_id = data.get('request_id')
        new_tts = data.get('new_tts')

        if not request_id or new_tts is None:
            return jsonify({"error": "Missing request_id or new_tts value"}), 400

        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Fetch the record based on the request_id
        cursor.execute("SELECT * FROM ApprovalRequestsWithDetails WHERE id = ?", request_id)
        record = cursor.fetchone()

        if not record:
            return jsonify({"error": "Record not found"}), 404

        # Convert Decimal values to float
        gsv = float(record.gsv)
        cogs = float(record.cogs)

        # Perform calculations
        new_tts_per = float(new_tts / 100)
        new_tts_value = round(new_tts_per * gsv,2)
        new_to_value = round(gsv - new_tts_value,2)
        new_gp_value = round(new_to_value - cogs,2)
        new_gm_value = round((new_gp_value / new_to_value) * 100,2 ) # Convert to percentage

        # Prepare updated values
        updated_values = {
            "new_tts": new_tts_value,
            "new_to": new_to_value,
            "new_gp": new_gp_value,
            "new_gm": new_gm_value,  # Already in percentage form
        }

        conn.close()

        return jsonify({"success": True, "updated_values": updated_values}), 200

    except Exception as e:
        print("Error calculating new results:", e)
        return jsonify({"error": "Failed to calculate new results"}), 500


@app.route('/calculate_new_rsp_results', methods=['POST'])
@login_required
def calculate_new_rsp_results():
    try:
        data = request.get_json()
        request_id = data.get('request_id')
        new_rsp = data.get('new_rsp')

        if not request_id or new_rsp is None:
            return jsonify({"error": "Missing request_id or new_rsp value"}), 400

        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Fetch the record based on the request_id
        cursor.execute("SELECT * FROM ApprovalRequestsWithDetails WHERE id = ?", request_id)
        record = cursor.fetchone()

        if not record:
            return jsonify({"error": "Record not found"}), 404

        # Explicitly convert Decimal values to float
        cogs = float(record.cogs)
        tts_per = float(record.tts_percentage) / 100
        vat = float(record.vat)
        pieces_per_case = float(record.pieces_per_case)
        rm = float(record.rm)
        wsm = float(record.wsm)
        dm = float(record.dm)
        duty = float(record.duty)
        clearing_charges = float(record.clearingcharges)
        bd = float(record.bd)
        cpp = float(record.cpp)

        # Perform calculations
        new_rsp_without_vat = new_rsp / (1 + vat)
        new_rsp_per_case = new_rsp_without_vat * pieces_per_case
        new_retail_markup = new_rsp_per_case / (1 + rm)
        new_bptt = new_retail_markup / (1 + wsm)
        new_dplc = new_bptt / (1 + dm)
        new_cif = (new_dplc / (1 + duty + clearing_charges)) - bd
        new_gsv = new_cif / (1 + cpp)

        new_tts_value = round(tts_per * new_gsv, 2)
        new_to_value = round(new_gsv - new_tts_value, 2)
        new_gp_value = round(new_to_value - cogs, 2)
        new_gm_value = round((new_gp_value / new_to_value) * 100, 2)  # Convert to percentage

        # Prepare updated values
        updated_values = {
            "new_bptt": new_bptt,
            "new_cif":new_cif,
            "new_gsv":new_gsv,
            "new_tts": new_tts_value,
            "new_to": new_to_value,
            "new_gp": new_gp_value,
            "new_gm": new_gm_value,  # Already in percentage form
        }

        conn.close()

        return jsonify({"success": True, "updated_values": updated_values}), 200

    except Exception as e:
        print("Error calculating new results:", e)
        return jsonify({"error": "Failed to calculate new results"}), 500



@app.route('/submit_request', methods=['POST'])
@login_required
def submit_request():
    if current_user.role != 'marketing':
        return "Unauthorized", 403

    data = request.get_json()

    sku_code = data['sku_code']
    country = data['country']
    rsp = (data['new_rsp'])
    tts_percentage = data['new_tts']
    bptt_new = data['bptt']
    cif_new = data["cif"]
    gsv_new = data["gsv"]
    to_new = data["to"]
    gp_new = data["gp"]
    gm_new = data["gm"]
    cogs_new = data["cogs"]
    sku_description = data["sku_description"]
    vat = data["newVat"]
    rm = data["newRM"]
    wsm = data["newWSM"]
    dm = data["newDM"]
    duty = data["newDuty"]
    cc = data["newCC"]
    bd = data["newBD"]
    cpp = data["newCPP"]
    pcs = data["pcs"]
    rsp_per_case = data["new_rsp_per_case"]
    

    
    # print(vat)
    
    # print(sku_description)

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch approvers
    cursor.execute("SELECT id, username FROM users WHERE role = 'ttsapprover'")
    finance = cursor.fetchone() # current approver
    
    cursor.execute("""
        SELECT CD_Manager
        FROM CountryDetails
        WHERE Country = ?""", 
        country)
    
    
    cd_manager_name = cursor.fetchone()
    
    cursor.execute("""
        SELECT id 
        FROM users 
        WHERE role = 'cdmanager' AND name = ?
    """, cd_manager_name[0])
    cd_manager_row = cursor.fetchone()
    
    if not cd_manager_row:
        return jsonify({"error": f"No CD manager found for country {country}"}), 400

    cd_manager_id = cd_manager_row[0]

    # cursor.execute("SELECT id, username FROM users WHERE role = 'cogsapprover'")
    # cogs_approver = cursor.fetchone()
    
    if not finance:
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
        cursor.execute("""DELETE FROM ApprovalRequestsWithDetails  
                          WHERE id = ?""", existing_id)

    # Insert the new request for TTS approval
    cursor.execute(
    """
    INSERT INTO ApprovalRequestsWithDetails 
        (sku_code, country, requester_id, current_approver_id, approver_name, rsp, 
         tts_percentage, status, bptt, cif, gsv, too, gp, gm, cogs, 
         requester_name, approval_type, next_approver_id, request_id, sku_description,
         vat, rm, wsm, dm, duty, [clearingcharges], bd, cpp, pieces_per_case, [RSP/Cs_LC])
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?)
    """,
    (
        sku_code, country, current_user.id, finance[0], finance[1], rsp, tts_percentage,
        'Pending', bptt_new, cif_new, gsv_new, to_new, gp_new, gm_new, cogs_new,
        current_user.username, 'TTS', cd_manager_id, request_id, sku_description, 
        vat, rm, wsm, dm, duty, cc, bd, cpp, pcs, rsp_per_case
    )
)


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
        SET status = 'TTS Approved', updated_at = GETDATE(), approval_type = 'CD Manager Approval', 
        current_approver_id = next_approver_id,
        next_approver_id = (
            SELECT id FROM users WHERE role = 'manager'
            ), 
        approver_name = (
            SELECT name FROM users WHERE role = 'ttsapprover'
        ), 
        request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'TTS'
    """, request_id_code, request_id, current_user.id)

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

@app.route('/change_tts', methods=['POST'])
@login_required
def change_tts():
    if current_user.role != 'ttsapprover':
        return "Unauthorized", 403

    data = request.get_json()
    request_id = data['request_id']
    new_tts = data['new_tts']
    new_to = data['new_to']
    new_gp = data['new_gp']
    new_gm = data['new_gm']


    

    if new_tts > 100:
        return jsonify({"error": "TTS percentage cannot exceed 100"}), 400

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Fetch request details
    cursor.execute("""
        SELECT requester_id, next_approver_id FROM ApprovalRequestsWithDetails 
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'TTS'
    """, request_id, current_user.id)
    request_data = cursor.fetchone()

    if not request_data:
        conn.close()
        return jsonify({"error": "Request not found or unauthorized"}), 404

    requester_id, next_approver_id = request_data

    # Update the TTS % and set current_approver_id to requester_id
    cursor.execute("""
        UPDATE ApprovalRequestsWithDetails
        SET tts_percentage = ?, 
            too = ?,
            gp = ?,
            gm = ?, 
            status = 'Updated TTS%', 
            approval_type= 'TTS Updated',
            current_approver_id = ?, 
            updated_at = GETDATE()
        WHERE id = ?
    """, new_tts, new_to, new_gp, new_gm, requester_id, request_id)

    conn.commit()
    conn.close()

    return jsonify({"message": "TTS updated successfully"}), 200

@app.route('/approve_new_tts', methods=['POST'])
@login_required
def approve_new_tts():
    data = request.json
    request_id = data.get('request_id')

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE ApprovalRequestsWithDetails
            SET status = 'TTS Approved', updated_at = GETDATE(), 
            approval_type = 'CD Manager Approval', 
            current_approver_id = next_approver_id, 
            approver_name = (
            SELECT name FROM users WHERE id = ?
            ), 
            next_approver_id = (
            SELECT id FROM users WHERE role = 'manager'
            )
            WHERE id = ?
        """, current_user.id, request_id)
        conn.commit()
        return jsonify({"message": "New TTS approved"}), 200
    except Exception as e:
        print("Error approving TTS:", e)
        return jsonify({"error": "Failed to approve TTS"}), 500
    finally:
        conn.close()

@app.route('/change_rsp', methods=['POST'])
@login_required
def change_rsp():
    data = request.json
    request_id = data.get('request_id')
    new_rsp = data.get('new_rsp')
    new_bptt = data.get('new_bptt')
    new_cif = data.get('new_cif')
    new_gsv = data.get('new_gsv')
    new_to = data.get('new_to')
    new_gp = data.get('new_gp')
    new_gm = data.get('new_gm')


    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE ApprovalRequestsWithDetails
            SET rsp = ?, bptt = ?, cif = ?, gsv = ?,
            too = ?, gp = ?, gm = ?,
            status = 'TTS Approved', 
            updated_at = GETDATE(), 
            approval_type = 'CD Manager Approval', 
            approver_name = (
            SELECT name FROM users WHERE id = ?
            ), 
            current_approver_id = next_approver_id, 
            next_approver_id = (
            SELECT id FROM users WHERE role = 'manager'
            )
            WHERE id = ?
        """, new_rsp, new_bptt, new_cif, new_gsv, new_to, new_gp, new_gm, current_user.id, request_id)
        conn.commit()
        return jsonify({"message": "RSP updated"}), 200
    except Exception as e:
        print("Error updating RSP:", e)
        return jsonify({"error": "Failed to update RSP"}), 500
    finally:
        conn.close()

@app.route('/close_request', methods=['POST'])
@login_required
def close_request():
    data = request.json
    request_id = data.get('request_id')

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE ApprovalRequestsWithDetails
            SET status = 'INACTIVE', current_approver_id = null
            WHERE id = ?
        """, request_id)
        conn.commit()
        return jsonify({"message": "Request closed"}), 200
    except Exception as e:
        print("Error closing request:", e)
        return jsonify({"error": "Failed to close request"}), 500
    finally:
        conn.close()
  
    


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
            "gm": row[14],
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
        SET status = 'Approved', approval_type = 'FinalApproval',
            updated_at = GETDATE(),
            current_approver_id = next_approver_id, 
            next_approver_id = null, 
            approver_name = (
                SELECT name FROM users WHERE id = ?
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'CD Manager Approval'
    """, current_user.id, request_id_code, request_id, current_user.id)
    
    conn.commit()
    conn.close()
    
    return jsonify({"message": "Approval approved successfully!"}), 200

@app.route("/reject_pre_final", methods=["POST"])
@login_required
def reject_pre_final():
    if current_user.role != 'cdmanager':
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
                SELECT name FROM users WHERE id = ?
            ), 
            request_id = ?
        WHERE id = ? AND current_approver_id = ? AND approval_type = 'Final Approval'
    """, current_user.id, request_id_code, request_id, current_user.id)

    conn.commit()
    conn.close()

    return jsonify({"message": "Request rejected successfully!"}), 200

    
@app.route("/approve_final", methods=["POST"])
@login_required
def final_approval():
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
        SELECT country, sku_code, tts_percentage, bptt, cif, [RSP/Cs_LC]
        FROM ApprovalRequestsWithDetails 
        WHERE id = ?
    """, request_id)
    request_details = cursor.fetchone()
    
    if request_details:
        country, sku_code, tts_percentage, bptt, cif, rsp_per_case = request_details
        tts_percentage = tts_percentage / 100
        cursor.execute("""
            UPDATE SKU_tts 
            SET [TTS%] = ?
            WHERE [SKU Code] = ?
        """, tts_percentage, sku_code)
        
        if country == 'Qatar':
            cursor.execute("""
            UPDATE [Qatar_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
        
        elif country == 'Kuwait':
            cursor.execute("""
            UPDATE [Kuwait_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
            
        elif country == 'KSA':
            cursor.execute("""
            UPDATE [KSA_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
            
        elif country == 'UAE':
            cursor.execute("""
            UPDATE [UAE_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
        
        elif country == 'Oman':
            cursor.execute("""
            UPDATE [Oman_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
            
        elif country == 'Bahrain':
            cursor.execute("""
            UPDATE [Bahrain_PS_New]
            SET [BPTT LC/Case] = ?, [CIF LC/case] = ?, [RSP/Cs_LC] = ?
            WHERE [SKU Code] = ?
            """, bptt, cif, rsp_per_case, sku_code)
            
        
        
    
    
    conn.commit()
    conn.close()
    
    return jsonify({"message": "Request approved and TTS, BPTT, and CIF uploaded successfully!"}), 200


@app.route("/reject_final", methods=["POST"])
@login_required
def final_reject():
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
    unique_id = request.args.get("unique_id")
    if not unique_id:
        return jsonify({"error": "Missing unique_id parameter"}), 400
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Fetch primary record
        cursor.execute("""
            SELECT sku_code, country, updated_at, rsp, cif, bptt, [RSP/Cs_LC]
            FROM ApprovalRequestsWithDetails
            WHERE id = ?
        """, unique_id)
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "No data found for the given unique_id"}), 404

        sku_code, country, updated_at, rsp, cif, bptt, rsp_per_case = result

        # Fetch distributor
        cursor.execute("""
            SELECT DB_Manager
            FROM CountryDetails
            WHERE Country = ?
        """, country)
        distributor = cursor.fetchone()
        distributor_name = distributor[0] if distributor else "Unknown Distributor"

        # Country-specific table mappings
        country_table_mapping = {
            "Qatar": "Qatar_PS_New",
            "Kuwait": "Kuwait_PS_New",
            "KSA": "KSA_PS_New",
            "Bahrain": "Bahrain_PS_New",
            "Oman": "Oman_PS_New",
            "UAE": "UAE_PS_New",
        }

        if country not in country_table_mapping:
            return jsonify({"error": f"No pricing data available for {country}"}), 400

        # Fetch country-specific data
        table_name = country_table_mapping[country]
        cursor.execute(f"""
            SELECT [Enitity], [SKU Description], [DB SKU], [VAT %], [VAT], [RM %], 
                   [Retail Markup LC], [Retail Price LC], [WSM %], [W/Sale Markup LC], 
                   [DM %], [Distributor Markup LC], [DPLC LC/case], [Duty %], [Duty], 
                   [Clearing Charges %], [Clearing Charges]
            FROM {table_name}
            WHERE [SKU Code] = ?
        """, sku_code)
        field_values = cursor.fetchone()

        if not field_values:
            return jsonify({"error": "No data found for the given SKU code"}), 404

        # Unpack the values
        (entity, desc, db_sku, vat_per, vat, rm_per, retail_markup_lc, retail_price, 
         wsm_per, w_sale, dm_per, distributor_markup, dplc, duty_per, duty, cc_per, cc) = field_values

        # Handle country-specific logic
        currency = "USD"
        sales_organization = 3330  # Default value

        if country == "Qatar":
            # Determine currency and sales organization based on pack type
            cursor.execute("""
                SELECT [Pack_Type]
                FROM SKU_Master$ExternalData_2
                WHERE [Material_Code] = ?
            """, sku_code)
            pack_type = cursor.fetchone()
            if pack_type and pack_type[0] == "Tea Bags":
                sales_organization = 5800
                currency = "USD"
            else:
                currency = "QAR"

        elif country == "Kuwait":
            currency = "KWD"
        
        elif country == "UAE":
            sales_organization = 3300

        # Format percentages and round values
        def format_percent(value):
            return f"{value * 100:.0f}%" if value is not None else None

        formatted_values = {
            "vat_per": format_percent(vat_per),
            "rm_per": format_percent(rm_per),
            "wsm_per": format_percent(wsm_per),
            "dm_per": format_percent(dm_per),
            "duty_per": format_percent(duty_per),
            "cc_per": format_percent(cc_per),
        }

        rounded_values = [
            round(v, 2) if isinstance(v, (int, float)) else v
            for v in [rsp_per_case, rm_per, retail_markup_lc, retail_price, wsm_per, 
                      w_sale, dm_per, distributor_markup, dplc, duty, cc]
        ]

        # Unpack rounded values
        rsp_lc, rm_per, retail_markup_lc, retail_price, wsm_per, w_sale, dm_per, \
        distributor_markup, dplc, duty, cc = rounded_values

        # Prepare the final data structure
        formatted_date = updated_at.strftime("%Y-%m-%d")
        data = {
            "date_of_communication": formatted_date,
            "distributor": distributor_name,
            "country": country,
            "effective_date_from": formatted_date,
            "effective_date_till": "Till next communication of Price update",
            "invoicing_currency": currency,
            "invoicing_entity": sales_organization,
            "finance_member": "Kunal Thakwani",
            "table_data": [
                [
                    sku_code, db_sku, desc, 0, formatted_values["vat_per"], vat, rsp, rsp_per_case,
                    formatted_values["rm_per"], retail_markup_lc, retail_price, formatted_values["wsm_per"],
                    w_sale, bptt, formatted_values["dm_per"], distributor_markup, dplc, 
                    formatted_values["duty_per"], duty, formatted_values["cc_per"], cc, cif
                ]
            ]
        }

        # Generate and send the PDF
        output_file = generate_price_structure_pdf(data)
        return send_file(output_file, as_attachment=current_user.role == "admin")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


   
    
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
    



@app.route("/download_excel_file/<country>", methods=['GET'])
@login_required
def download_file(country):
    if current_user.role != 'admin':
        return "Unauthorized", 403 
    
    allowed_countries = ["Qatar_PS_New", "Bahrain_PS_New", "Oman_PS_New", "Kuwait_PS_New", "KSA_PS_New", "UAE_PS_New"]
    
    if country not in allowed_countries:
        return "Invalid country", 400
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Manually build the query string with the validated table name
    query = f"""
        SELECT TOP (1000) [DD code],
        [Enitity], [SKU Code], [Comments], [DB SKU], [CPD code],
        [SKU Description], [Brand], [Sector], [Flavor], [Format],
        [Packing], [Project], [Type], [SU], [Cases/Ton], [Units/Cs],
        [Valid from], [Valid to], [Proposed RSP (inc VAT) LC], [VAT %],
        [VAT], [Proposed RSP (ex VAT) LC], [RSP/Cs_LC], [RM %],
        [Retail Markup LC], [Retail Price LC], [WSM %],
        [W/Sale Markup LC], [BPTT LC/Case], [DM %], [Distributor Markup LC],
        [DPLC LC/case], [Duty %], [Duty], [Clearing Charges %],
        [Clearing Charges], [BD], [CIF LC/case], [CPP%], [CPP],
        [GSV LC/case], [F43], [Stock], [Z521 SAP], [F46], [F47],
        [BPTT $/Case], [CIF $/Case], [BPTT $/Ton], [CIF $/Ton],
        [GSV/Ton $], [BPTT LC/Piece], [Check], [Z009], [Z521], [Z000]
        FROM {country}
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    except Exception as e:
        return f"Error executing query: {str(e)}", 500
    finally:
        conn.close()
    
    # Convert results to a DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)
    
    # Export to an in-memory Excel file
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    output.seek(0)
    
    # Send the file as a response
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"{country}.xlsx",
    )




if __name__ == '__main__':
    app.run(debug=True)