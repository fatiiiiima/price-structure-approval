# import bcrypt

# def hash_password(password):
#     # Generate salt and hash
#     salt = bcrypt.gensalt()
#     hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
#     return hashed.decode('utf-8')

# admin_password = "PriceStructureAdmin@123"  # Replace with the desired admin password
# hashed_password = hash_password(admin_password)

# print("Hashed Password:", hashed_password)


import secrets

secret_key = secrets.token_hex(16)  # 16 bytes = 32 hex characters
print(secret_key)




# <!-- <!DOCTYPE html>
# <html lang="en">
# <head>
#   <meta charset="UTF-8">
#   <meta name="viewport" content="width=device-width, initial-scale=1.0">
#   <title>Price Process</title>
  
#   <link 
#     href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" 
#     rel="stylesheet">
# </head>
# <body>
#   <div class="container mt-5">
#     <h1>Price Structure</h1>
#     <form>
#       <div class="form-group">
#         <label for="exampleInputEmail1">SKU Number</label>
#         <select class="form-control" id="exampleFormControlSelect1">
#           <option>1</option>
#           <option>2</option>
#           <option>3</option>
#           <option>4</option>
#           <option>5</option>
#         </select>
#       </div>
#       <div class="form-group">
#         <label for="exampleInputEmail1">Country</label>
#         <select class="form-control" id="exampleFormControlSelect1">
#           <option>KSA</option>
#           <option>UAE</option>
#           <option>Kuwait</option>
#           <option>Qatar</option>
#           <option>Oman</option>
#           <option>Bahrain</option>
#         </select>
#       </div> -->
# <!-- <div class="form-group">
#         <label for="exampleInputPassword1">Price</label>
#         <input 
#           type="password" 
#           class="form-control" 
#           id="exampleInputPassword1" 
#           placeholder="Price">
#       </div> -->
# <!-- <button type="submit" class="btn btn-primary">Submit</button>
#     </form>
#   </div>
# </body>
# </html> -->
