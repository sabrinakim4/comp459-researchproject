#!/usr/bin/env python
# coding: utf-8

# In[182]:


import pandas as pd
from collections import defaultdict
import time


# In[183]:


business_json_path = 'yelp_academic_dataset_business.json'
business_df = pd.read_json(business_json_path, lines=True)

# 1 = open, 0 = closed
business_df = business_df[business_df['is_open']==1]
drop_columns = ['hours','is_open', 'name', 'address', 'state', 'postal_code', 'latitude', 
                'longitude', 'stars', 'is_open', 'attributes', 'hours']
business_df = business_df.drop(drop_columns, axis=1)
restaurant_df = business_df[business_df['categories'].str.contains('Restaurant', case=False, na=False)]


# In[184]:


# ##### figuring out which city to use --> decided Philidephia
cities = defaultdict(int)

for index, row in restaurant_df.iterrows():
    cities[row['city']] += 1
sorted_cities = sorted(cities.items(), key = lambda x: x[1], reverse=True)
    
print('cities:', (sorted_cities))
    
most_popular_count = 0
city_to_use = ''

for city in cities:
    num_restaurants = cities[city]
    if num_restaurants > most_popular_count:
        city_to_use = city
        most_popular_count = num_restaurants
    
print(cities)
print('city:', city_to_use)
print('# restaurants:', cities[city_to_use])


# In[185]:


city = 'Tampa'
restaurant_df = restaurant_df.drop(restaurant_df[restaurant_df['city'] != city].index)


# In[186]:


print("number of rows", len(restaurant_df))
print(restaurant_df.head())


# In[187]:


get_ipython().run_line_magic('store', 'restaurant_df')


# In[ ]:


##### figuring out which restaurant features to use --> decided to use all of them 
# categories = defaultdict(int)

# for index, row in restaurant_df.iterrows():
#     items = row['categories'].split(", ")
#     for cat in items:
#         categories[cat] += 1

# print(categories)
# print(len(categories))


# In[188]:


review_json_path = 'yelp_academic_dataset_review.json'
size = 1000000
# size = 100
review = pd.read_json(review_json_path, lines=True,
                      dtype={'review_id':str,'user_id':str,
                             'business_id':str,'stars':int,
                             'date':str,'text':str,'useful':int,
                             'funny':int,'cool':int},
                      chunksize=size)


# In[189]:


# restore restaurant_df before we merge review data with philidelphia restaurants
get_ipython().run_line_magic('store', '-r restaurant_df')


# In[190]:


start_time = time.time()
chunk_list = []

# data_size = 3000
seed = 0

for chunk_review in review:

    # dropping unnecessary columns
    chunk_review = chunk_review.drop(['review_id','useful','funny','cool', 'text', 'date', 'text'], axis=1)

    chunk_merged = pd.merge(restaurant_df, chunk_review, on='business_id', how='inner')

    chunk_list.append(chunk_merged)

# After trimming down the review file, concatenate all relevant data back to one dataframe
reviewed_restaurants_df = pd.concat(chunk_list, ignore_index=True, join='outer', axis=0)
print("this took", time.time() - start_time, "to run")


# In[191]:


print(reviewed_restaurants_df.head())
print('num of reviewed restaurants in one city:', reviewed_restaurants_df.shape[0])


# In[139]:


get_ipython().run_line_magic('store', 'reviewed_restaurants_df')


# In[192]:


#### loading yelp user data
user_json_path = 'yelp_academic_dataset_user.json'
user = pd.read_json(user_json_path, lines=True, chunksize=1000000)


# In[193]:


start_time = time.time()

chunk_list = [] # list of dataframes
# get all users in dataframe (user_id, ...) -->
for chunk_user in user:

    # dropping unnecessary columns
    chunk_user = chunk_user.drop(['name', 'yelping_since', 'friends', 'useful', 'funny',
                                 'cool', 'fans', 'compliment_hot', 'compliment_more', 
                                 'compliment_profile', 'compliment_cute', 'compliment_list',
                                 'compliment_note', 'compliment_plain', 'compliment_cool', 
                                 'compliment_funny', 'compliment_writer', 'compliment_photos'], axis=1)
    
    chunk_user = chunk_user.rename(columns={'review_count': 'user_review_count'})
    
    chunk_merged = pd.merge(reviewed_restaurants_df, chunk_user, on='user_id', how='inner')

    chunk_list.append(chunk_merged)
    
    chunk_list.append(chunk_merged)

reviewed_restaurants_w_user_df = pd.concat(chunk_list, ignore_index=True) # ignore_index means indices will be 0 -> n-1

print("this took", time.time() - start_time, "to run")


# In[199]:


print(reviewed_restaurants_w_user_df.shape[0])
print(reviewed_restaurants_w_user_df.head())


# In[200]:


get_ipython().run_line_magic('store', 'reviewed_restaurants_w_user_df')


# In[201]:


shorter_reviewed_restaurants_w_user_df = reviewed_restaurants_w_user_df[reviewed_restaurants_w_user_df['user_review_count'] > 1000]
shorter_reviewed_restaurants_w_user_df = shorter_reviewed_restaurants_w_user_df[shorter_reviewed_restaurants_w_user_df['review_count'] > 1000]


# In[202]:


print(shorter_reviewed_restaurants_w_user_df.shape[0])
print(shorter_reviewed_restaurants_w_user_df.head())


# In[203]:


get_ipython().run_line_magic('store', 'shorter_reviewed_restaurants_w_user_df')


# <!-- print(all_users_df.head()) -->

# In[98]:


# %store all_users_df


# In[119]:


# all_active_users_df = all_users_df[all_users_df['review_count'] > 600]


# In[121]:


# %store all_active_users_df


# In[120]:


# print(all_active_users_df.shape[0])
# print(all_active_users_df.head())


# In[194]:


get_ipython().run_line_magic('store', '-r shorter_reviewed_restaurants_w_user_df')


# In[204]:


# getting list of restaurant categories
restaurant_cats = set()
for index, row in shorter_reviewed_restaurants_w_user_df.iterrows():
    cats = row['categories'].split(", ")
    for cat in cats:
        restaurant_cats.add(cat)

restaurant_cats = list(restaurant_cats)

# map restaurant cat to idx in this list
restaurant_cat_idx = {}

for index, cat in enumerate(restaurant_cats):
    restaurant_cat_idx[cat] = index
    
number_cats = len(restaurant_cats)



# In[205]:


print("number of restaurant categories:", number_cats)


# In[206]:


# M matrix (gives us edges to bipartite graph)
M = defaultdict(list)

user_counter = 0
restaurant_counter = 0

# mapping of user id to user index
seen_user_ids = {}
# mapping of business id to item index
seen_restaurant_ids = {}

# largest_restaurant_idx = 0

for index, row in shorter_reviewed_restaurants_w_user_df.iterrows():
    
    user_idx = user_counter
    if row['user_id'] in seen_user_ids:
        user_idx = seen_user_ids[row['user_id']]
    else:
        seen_user_ids[row['user_id']] = user_counter
        user_counter += 1
        
    restaurant_idx = restaurant_counter
    if row['business_id'] in seen_restaurant_ids:
        restaurant_idx = seen_restaurant_ids[row['business_id']]
    else:
        seen_restaurant_ids[row['business_id']] = restaurant_counter
        restaurant_counter += 1
    
#     largest_restaurant_idx = max(largest_restaurant_idx, restaurant_idx)
        
#         #### new restaurant
#         restaurant_feature_row = [restaurant_idx]
#         marked_restaurant_categories = [0] * number_cats

#         for cat in row['categories'].split(", "):
#             marked_restaurant_categories[restaurant_cat_idx[cat]] = 1 # setting cat to 1

#         restaurant_feature_row.extend(marked_restaurant_categories)
#         # adding new row to restaurant feature matrix
#         restaurant_feature_M.append(restaurant_feature_row)
    
    rating = row['stars']
        
    M['u_nodes'].append(user_idx)
    M['v_nodes'].append(restaurant_idx)
    M['ratings'].append(rating)    
# f.close()

# print('largest restauarnt_idx:', largest_restaurant_idx)


# In[207]:


print('# users:', len(seen_user_ids))
print('# items:', len(seen_restaurant_ids))



# In[209]:


M_df = pd.DataFrame(M)


# In[210]:


print(M_df.shape[0])
print(M_df.head())


# In[211]:


# restaurant feature matrix: categories
restaurant_feature_M = [None] * len(seen_restaurant_ids)
# user feature matrix: review_count, num years elite, avg stars
user_feature_M = {}
review_count_rows = [None] * len(seen_user_ids)
years_elite_rows = [None] * len(seen_user_ids)
avg_stars_rows = [None] * len(seen_user_ids)

for index, row in shorter_reviewed_restaurants_w_user_df.iterrows():
    user_idx = seen_user_ids[row['user_id']]
    restaurant_idx = seen_restaurant_ids[row['business_id']]
    
    if restaurant_feature_M[restaurant_idx] == None:
        restaurant_feature_vector = [0] * number_cats
        for cat in row['categories'].split(", "):
            restaurant_feature_vector[restaurant_cat_idx[cat]] = 1 # setting cat to 1
        
        restaurant_feature_M[restaurant_idx] = restaurant_feature_vector
        
    if review_count_rows[user_idx] == None:
        review_count_rows[user_idx] = row['review_count']
        years_elite_rows[user_idx] = len(row['elite'].split(','))
        avg_stars_rows[user_idx] = row['average_stars']
        
user_feature_M['review_count'] = pd.Series(review_count_rows, dtype='int32')
user_feature_M['years_elite'] = pd.Series(years_elite_rows, dtype='int32')
user_feature_M['avg_stars'] = pd.Series(avg_stars_rows, dtype='float32')
        


# In[212]:


user_feature_M_df = pd.DataFrame(user_feature_M)
restaurant_feature_M_df = pd.DataFrame(restaurant_feature_M)


# In[213]:


print(user_feature_M_df.shape[0])
print(user_feature_M_df.head())
print(restaurant_feature_M_df.shape[0])
print(restaurant_feature_M_df.head())


# In[214]:


get_ipython().run_line_magic('store', 'M_df')
get_ipython().run_line_magic('store', 'user_feature_M_df')
get_ipython().run_line_magic('store', 'restaurant_feature_M_df')


# In[32]:


# M_output_file = open("mdata.txt", "a")
# for index, row in M_df.iterrows():
#     print(row['u_nodes'], row['v_nodes'], row['ratings'], file=M_output_file)
# M_output_file.close()


# In[215]:


M_df.to_csv("yelp_M_data.csv", index=False, sep='\t', header=False)
user_feature_M_df.to_csv("yelp_user_feature_M_data.csv", index=False, sep='\t', header=False)
restaurant_feature_M_df.to_csv("yelp_restaurant_feature_M_data.csv", index=False, sep='\t', header=False)


# In[ ]:




