# from flask import Flask,request,jsonify
# import pandas as pd

# app = Flask(__name__)

# def get_product_details(data,name=False):
#     product_df = pd.read_csv('products.csv')
#     for index,row in data.iterrows():
#         products_df = product_df[product_df['UPC'] == row['UPC']]
#         data.loc[index,'Product_Name'] = products_df['Product_Name'].values[0]
#         data.loc[index,'Category'] = products_df['Category'].values[0]
#         data.loc[index,'Description'] = products_df['Description'].values[0]

# def get_user_deatils(tn):
#     user_df = pd.read_csv('token_audience_mapping.csv')
#     data = user_df[user_df['Token'] == tn]

#     return data

# def get_purchase_details(user_id):

#     purchase_df = pd.read_csv('product_purchase_details.csv')
#     data = purchase_df[purchase_df['User_ID'] == user_id]
#     get_product_details(data)
#     return data

# def get_recent_search_products(user_id):
#     recent_df = pd.read_csv('recent_search_products.csv')
#     data = recent_df[recent_df['User_ID'] == user_id]

#     get_product_details(data)
#     return data

# def get_location(recent):
#     product_df = pd.read_csv('products.csv')
#     l = []
#     loc_df = pd.read_csv('product_images.csv')


#     for index, row in recent.iterrows():


#         data = loc_df[loc_df['UPC'] == row['UPC']]['Image_File_Path']
#         l.append(dict())
#         l[index][product_df[product_df['UPC'] == row['UPC']]['Product_Name'].values[0]] = data.tolist()
#     return l

# def get_product_offer():
#     offer_df = pd.read_csv('products_offers.csv')
#     get_product_details(offer_df)
#     return offer_df

# def next_best_product(user_id):
#     recent_df = pd.read_csv('recent_search_products.csv')
#     offer_df = pd.read_csv('products_offers.csv')
#     product_df=pd.read_csv('products.csv')
#     order_df=pd.read_csv("product_purchase_details.csv")

#     # data = recent_df[recent_df['User_ID'] == user_id]
#     #
#     # ord_data =order[order['User_ID'] == user_id]
#     #
#     # new=data["UPC"]
#     # print(ord_data["UPC"])
#     #
#     # matched_products = product[product["UPC"].isin(new)]
#     # Category_data = matched_products["Category"].unique()  # Extract unique categories
#     #
#     # # Filter products that belong to these categories
#     # category_products = product[product["Category"].isin(Category_data)]
#     #
#     # print(category_products)
#     #
#     #
#     # val=category_products.merge(offer_df, on='UPC', how='left')
#     #
#     #
#     #
#     #
#     # get_product_details(val)
#     recent_data = recent_df[recent_df['User_ID'] == user_id]

#     # Get products the user purchased
#     order_data = order_df[order_df['User_ID'] == user_id]

#     # Combine UPCs from both recent searches and orders
#     user_upcs = set(recent_data["UPC"]).union(set(order_data["UPC"]))

#     # Filter products that match these UPCs
#     matched_products = product_df[product_df["UPC"].isin(user_upcs)]

#     # Get unique categories of matched products
#     categories = matched_products["Category"].unique()

#     # Filter products that belong to these categories
#     category_products = product_df[product_df["Category"].isin(categories)]

#     # Merge with offers
#     final_data = category_products.merge(offer_df, on='UPC', how='left')

#     return final_data



# def product_management(tokens):
#     token = tokens
#     user = get_user_deatils(token).reset_index(drop=True)
#     purchase = get_purchase_details(user['User_ID'].values[0]).reset_index(drop=True)

#     offer = get_product_offer()
#     recent_search = get_recent_search_products(user['User_ID'].values[0]).reset_index(drop=True)
#     search_location = get_location(recent_search)
#     purchase_location = get_location(purchase)
#     best_product=next_best_product(user['User_ID'].values[0])


#     response = {
#         '1.User_Details' : user.to_dict(orient='records'),
#         '2.Purchase_Details' : purchase.to_dict(orient='records'),
#         '3.Recently_searched_products' : recent_search.to_dict(orient='records'),
#         '4.searched_product_location' : search_location,
#         '5.purchased_product_location' : purchase_location,
#         #'6.Offers_availability' : offer.to_dict(orient='records'),

#         '7.next best product':best_product.to_dict(orient='records')

#     }


#     return jsonify(response)



# app.add_url_rule(rule='/product/<tokens>',view_func=product_management,methods=['GET'])

# if __name__ == '__main__':
#     app.run(debug=True)


from flask import Flask, request, jsonify
import pandas as pd
import redis
import json

app = Flask(__name__)

# Connect to Redis (Ensure Redis is running on port 6379)
cache = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def get_product_details(data):
    product_df = pd.read_csv('products.csv')
    for index, row in data.iterrows():
        products_df = product_df[product_df['UPC'] == row['UPC']]
        if not products_df.empty:
            data.loc[index, 'Product_Name'] = products_df['Product_Name'].values[0]
            data.loc[index, 'Category'] = products_df['Category'].values[0]
            data.loc[index, 'Description'] = products_df['Description'].values[0]

def get_user_details(token):
    cache_key = f"user:{token}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return pd.DataFrame(json.loads(cached_data))
    
    user_df = pd.read_csv('token_audience_mapping.csv')
    data = user_df[user_df['Token'] == token]
    
    if not data.empty:
        cache.setex(cache_key, 600, data.to_json(orient='records'))  # Cache for 10 minutes
    
    return data

def get_purchase_details(user_id):
    cache_key = f"purchase:{user_id}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return pd.DataFrame(json.loads(cached_data))
    
    purchase_df = pd.read_csv('product_purchase_details.csv')
    data = purchase_df[purchase_df['User_ID'] == user_id]
    get_product_details(data)
    
    if not data.empty:
        cache.setex(cache_key, 600, data.to_json(orient='records'))  
    
    return data

def get_recent_search_products(user_id):
    cache_key = f"recent_search:{user_id}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return pd.DataFrame(json.loads(cached_data))
    
    recent_df = pd.read_csv('recent_search_products.csv')
    data = recent_df[recent_df['User_ID'] == user_id]
    get_product_details(data)
    
    if not data.empty:
        cache.setex(cache_key, 300, data.to_json(orient='records'))  # Cache for 5 minutes
    
    return data

@app.route('/product/<token>', methods=['GET'])
def product_management(token):
    user = get_user_details(token)
    if user.empty:
        return jsonify({"error": "User not found"}), 404
    
    user_id = user['User_ID'].values[0]
    
    purchase = get_purchase_details(user_id)
    recent_search = get_recent_search_products(user_id)
    
    response = {
        'User_Details': user.to_dict(orient='records'),
        'Purchase_Details': purchase.to_dict(orient='records'),
        'Recently_Searched_Products': recent_search.to_dict(orient='records')
    }
    
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
