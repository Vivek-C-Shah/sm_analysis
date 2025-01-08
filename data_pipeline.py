from astrapy import DataAPIClient
from dotenv import load_dotenv
import pandas as pd
import requests
import json
import os

class DataPipeline:
    def __init__(self):
        load_dotenv()
        self.client = DataAPIClient(os.getenv('ASTRADB_TOKEN'))
        self.db = self.client.get_database_by_api_endpoint(
            os.getenv('ASTRADB_ENDPOINT')
        )

    def read_csv(self, file_path):
        return pd.read_csv(file_path)

    def get_collection_data(self, collection_name, ):
        return self.db[collection_name].find({})

    def insert_data(self, collection_name, data):
        self.db[collection_name].insert_many(data)

    def delete_data(self, collection_name, query):
        self.db[collection_name].delete_many(query)

    def update_data(self, collection_name, query, data):
        self.db[collection_name].update_many(query, data)

    def list_collections(self):
        return self.db.list_collection_names()
    
    def get_collection(self, collection_name):
        return self.db[collection_name]
    
    def get_collection_count(self, collection_name):
        return self.db[collection_name].count_documents({})
    
    def get_collection_schema(self, collection_name):
        return self.db[collection_name].find_one()
    
    def get_engagement_metrics(self, data):
        df = pd.DataFrame(data)

        metrics_df = df.groupby("post_type").agg({
            "likes": "mean",
            "shares": "mean",
            "comments": "mean"
        }).reset_index()

        metrics_df.rename(columns={
            "likes": "avg_likes",
            "shares": "avg_shares",
            "comments": "avg_comments"
        }, inplace=True)
        metrics_df["engagement_rate"] = metrics_df["avg_likes"] + metrics_df["avg_shares"] + metrics_df["avg_comments"]
        metrics_df["like_to_comment_ratio"] = metrics_df["avg_likes"] / metrics_df["avg_comments"]
        metrics_df["share_to_like_ratio"] = metrics_df["avg_shares"] / metrics_df["avg_likes"]

        return metrics_df
    
    def get_engagement_metrics_by_date(self, data):

        df = pd.DataFrame(data)

        metrics_df = df.groupby("date").agg({
            "likes": "mean",
            "shares": "mean",
            "comments": "mean"
        }).reset_index()

        metrics_df.rename(columns={
            "likes": "avg_likes",
            "shares": "avg_shares",
            "comments": "avg_comments"
        }, inplace=True)
        metrics_df["engagement_rate"] = metrics_df["avg_likes"] + metrics_df["avg_shares"] + metrics_df["avg_comments"]
        metrics_df["like_to_comment_ratio"] = metrics_df["avg_likes"] / metrics_df["avg_comments"]
        metrics_df["share_to_like_ratio"] = metrics_df["avg_shares"] / metrics_df["avg_likes"]

        return metrics_df

    def get_engagement_metrics_by_post_type(self, data):

        df = pd.DataFrame(data)

        metrics_df = df.groupby("post_type").agg({
            "likes": "mean",
            "shares": "mean",
            "comments": "mean"
        }).reset_index()

        metrics_df.rename(columns={
            "likes": "avg_likes",
            "shares": "avg_shares",
            "comments": "avg_comments"
        }, inplace=True)
        metrics_df["engagement_rate"] = metrics_df["avg_likes"] + metrics_df["avg_shares"] + metrics_df["avg_comments"]
        metrics_df["like_to_comment_ratio"] = metrics_df["avg_likes"] / metrics_df["avg_comments"]
        metrics_df["share_to_like_ratio"] = metrics_df["avg_shares"] / metrics_df["avg_likes"]

        return metrics_df
    
    def get_engagement_metrics_by_date_and_post_type(self, data):
        df = pd.DataFrame(data)

        metrics_df = df.groupby(["date", "post_type"]).agg({
            "likes": "mean",
            "shares": "mean",
            "comments": "mean"
        }).reset_index()

        metrics_df.rename(columns={
            "likes": "avg_likes",
            "shares": "avg_shares",
            "comments": "avg_comments"
        }, inplace=True)
        metrics_df["engagement_rate"] = metrics_df["avg_likes"] + metrics_df["avg_shares"] + metrics_df["avg_comments"]
        metrics_df["like_to_comment_ratio"] = metrics_df["avg_likes"] / metrics_df["avg_comments"]
        metrics_df["share_to_like_ratio"] = metrics_df["avg_shares"] / metrics_df["avg_likes"]
        return metrics_df
    
  

    def generate_system_prompt(self):
        system_prompt = {
                "role": "You are an expert in social media analytics. Your role is to analyze performance data for various post types and provide actionable insights to improve engagement and performance.",
                "sample_data": {
                    "carousel": {
                        "average_likes": 2521.54,
                        "average_shares": 497.70,
                        "average_comments": 1006.64,
                        "engagement_rate": 4025.88,
                        "like_to_comment_ratio": 2.50,
                        "share_to_like_ratio": 0.20
                    },
                    "live_stream": {
                        "average_likes": 2326.84,
                        "average_shares": 506.98,
                        "average_comments": 919.35,
                        "engagement_rate": 3753.17,
                        "like_to_comment_ratio": 2.53,
                        "share_to_like_ratio": 0.22
                    },
                    "poll": {
                        "average_likes": 2671.08,
                        "average_shares": 487.13,
                        "average_comments": 1008.66,
                        "engagement_rate": 4166.86,
                        "like_to_comment_ratio": 2.65,
                        "share_to_like_ratio": 0.18
                    },
                    "reels": {
                        "average_likes": 2671.82,
                        "average_shares": 501.01,
                        "average_comments": 1037.46,
                        "engagement_rate": 4210.29,
                        "like_to_comment_ratio": 2.58,
                        "share_to_like_ratio": 0.19
                    },
                    "static_images": {
                        "average_likes": 2349.77,
                        "average_shares": 490.60,
                        "average_comments": 1059.26,
                        "engagement_rate": 3899.62,
                        "like_to_comment_ratio": 2.22,
                        "share_to_like_ratio": 0.21
                    },
                    "stories": {
                        "average_likes": 2877.52,
                        "average_shares": 493.26,
                        "average_comments": 955.45,
                        "engagement_rate": 4326.24,
                        "like_to_comment_ratio": 3.01,
                        "share_to_like_ratio": 0.17
                    }
                },
                "instructions": [
                "When a user provides a post type (e.g., 'carousel', 'reels', 'static images'), do the following:",
                "1. Analyze the performance of the specified post type based on the above metrics.",
                "2. Compare its performance to other post types to identify strengths and weaknesses.",
                "3. Provide actionable recommendations to improve engagement for that post type.",
                "4. Highlight patterns, opportunities, or potential issues."
                ]
            }
        return system_prompt

    
    def generate_payload(self, metrics_data, system_prompt, model="gemini"):
        data = pd.DataFrame(metrics_data)
        metrics_prompt = ""
        for index, row in data.iterrows():
            metrics_prompt += f"{index + 1}. {row['post_type'].title()}:\n"
            metrics_prompt += f"   - Average Likes: {row['avg_likes']:.2f}\n"
            metrics_prompt += f"   - Average Shares: {row['avg_shares']:.2f}\n"
            metrics_prompt += f"   - Average Comments: {row['avg_comments']:.2f}\n"
            metrics_prompt += f"   - Engagement Rate: {row['engagement_rate']:.2f}\n"
            metrics_prompt += f"   - Like-to-Comment Ratio: {row['like_to_comment_ratio']:.2f}\n"
            metrics_prompt += f"   - Share-to-Like Ratio: {row['share_to_like_ratio']:.2f}\n\n"

        payload = {
            "input_value": metrics_prompt,
            "output_type": "chat",
            "input_type": "chat",
            "tweaks": {
                "Prompt-AqR9k": {
                    "template": system_prompt
                },
                "GoogleGenerativeAIModel-eUrUo": {
                    "google_api_key": os.getenv('GEMINI_API_KEY'),
                    "input_value": "",
                    "max_output_tokens": None,
                    "model": "gemini-1.5-pro",
                    "n": None,
                    "stream": False,
                    "system_message": "",
                    "temperature": 0.1,
                    "top_k": None,
                    "top_p": None
                },
                "ChatOutput-Zpelg": {
                    "background_color": "",
                    "chat_icon": "",
                    "data_template": "{text}",
                    "input_value": "",
                    "sender": "Machine",
                    "sender_name": "AI",
                    "session_id": "",
                    "should_store_message": True,
                    "text_color": ""
                }
            }
        }
        return payload

    def get_engagement_insights(self, payload):
        HEADERS = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.getenv('LANGFLOW_TOKEN')}"
        }
        response = requests.post(os.getenv('LANGFLOW_ENDPOINT'), headers=HEADERS, json=payload)
        if response.status_code == 200:
            try:
                result = response.json()
                outputs = result.get("outputs", [])
                if outputs:
                    text = outputs[0].get("outputs", [{}])[0].get("results", {}).get("message", {}).get("text", "")
                    return text
                else:
                    return "No insights were generated by the Langflow API."
            except Exception as e:
                return f"Error parsing response: {str(e)}"
        else:
            return f"Error: {response.status_code} - {response.text}"
