from flask import Flask, request, render_template, redirect, url_for, jsonify, session
import pyodbc
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import bcrypt
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

app.secret_key = os.getenv('SECRET_KEY')


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


server = 'Factory'
database = 'TestDB'
conn_str = f"""
    DRIVER={{SQL Server}};
    SERVER={server};
    DATABASE={database};
    Trusted_Connection=yes;
"""

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

if __name__ == "__main__":
    app.run(debug=True)
