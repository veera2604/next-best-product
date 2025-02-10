from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

product_df = pd.read_csv('products.csv')
user_df = pd.read_csv('token_audience_mapping.csv')
purchase_df = pd.read_csv('product_purchase_details.csv')
recent_df = pd.read_csv('recent_search_products.csv')
offer_df = pd.read_csv('products_offers.csv')
loc_df = pd.read_csv('product_images.csv')


# Function to get product details
def get_product_details(data):
    return data.merge(product_df, on='UPC', how='left')


# Function to get user details from token
def get_user_details(token):
    return user_df[user_df['Token'] == token]


# Function to get purchase details for a user
def get_purchase_details(user_id):
    data = purchase_df[purchase_df['User_ID'] == user_id]
    return get_product_details(data)


# Function to get recent search products for a user
def get_recent_search_products(user_id):
    data = recent_df[recent_df['User_ID'] == user_id]
    val=recent_df["UPC"]
    r_val=val.mode().values[0]
    print(r_val)
    return get_product_details(data)


# Function to get product locations
def get_location(data):
    merged = data.merge(loc_df, on='UPC', how='left')[['Product_Name', 'Image_File_Path']]
    return merged.groupby('Product_Name')['Image_File_Path'].apply(list).to_dict()


# Function to get product offers
def get_product_offer():
    return get_product_details(offer_df)


def product_management():
    token = request.args.get('Token')
    if not token:
        return jsonify({"error": "Token is required"}), 400

    user = get_user_details(token)
    if user.empty:
        return jsonify({"error": "Invalid Token"}), 404

    user_id = user['User_ID'].values[0]
    purchase = get_purchase_details(user_id)
    recent_search = get_recent_search_products(user_id)

    response = {
        '1.User_Details': user.to_dict(orient='records'),
        '2.Purchase_Details': purchase.to_dict(orient='records'),
        '3.Recently_searched_products': recent_search.to_dict(orient='records'),
        '4.Searched_Product_Location': get_location(recent_search),
        '5.Purchased_Product_Location': get_location(purchase),
        '6.Offers_Availability': get_product_offer().to_dict(orient='records'),
    }

    return jsonify(response)


app.add_url_rule('/product', view_func=product_management, methods=['GET'])

if __name__ == '__main__':
    app.run(debug=True)
