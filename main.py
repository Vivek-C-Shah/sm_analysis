import pandas as pd
import requests
import json

# Load the dataset
metrics_df = pd.read_csv("final_metrics.csv")

# Define the Langflow API endpoint and token
url = "https://api.langflow.astra.datastax.com/lf/0d55ad7b-7f9b-40c6-b96e-178409018ffa/api/v1/run/373ef7aa-0ab4-4a5a-a75f-bb4edf496f33?stream=false"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer AstraCS:UgeLajXUdHFnjycldszinXSn:e03bed97715b20e1e266ea17ed4ba7c59736892bed8e2423a99a16d9d98c55a1"
}

# Function to send metrics for a specific post_type
def send_metrics(post_type):
    try:
        # Filter the metrics for the given post_type
        row = metrics_df[metrics_df["post_type"] == post_type].iloc[0]
        
        # Prepare the payload dynamically
        payload = {
            "output_type": "chat",
            "input_type": "chat",
            "tweaks": {
                "ChatInput-I77JO": {
                    "input_value": f"Metrics for {post_type}",
                    "sender": "User",
                    "sender_name": "User",
                    "session_id": "",
                    "should_store_message": True
                },
                "Prompt-8eRmY": {
                    "template": "Analyze the following data for social media post type: \"{post_type}\".\n\nMetrics:\n- Average Likes: {avg_likes}\n- Average Shares: {avg_shares}\n- Average Comments: {avg_comments}\n- Engagement Rate: {engagement_rate}\n- Like-to-Comment Ratio: {like_to_comment_ratio}\n- Share-to-Like Ratio: {share_to_like_ratio}\n\nBased on these metrics, provide actionable insights to improve engagement and performance.\n",
                    "post_type": row["post_type"],
                    "avg_likes": row["avg_likes"],
                    "avg_shares": row["avg_shares"],
                    "avg_comments": row["avg_comments"],
                    "engagement_rate": row["engagement_rate"],
                    "like_to_comment_ratio": row["like_to_comment_ratio"],
                    "share_to_like_ratio": row["share_to_like_ratio"]
                },
                "OpenAIModel-MIPkw": {
                    "api_key": "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",
                    "input_value": "",
                    "json_mode": False,
                    "max_tokens": None,
                    "model_kwargs": {},
                    "model_name": "gpt-4o-mini",
                    "openai_api_base": "",
                    "output_schema": {},
                    "seed": 1,
                    "stream": False,
                    "system_message": "",
                    "temperature": 0.1
                },
                "ChatOutput-Cd9Oa": {
                    "data_template": "{text}",
                    "sender": "Machine",
                    "sender_name": "AI",
                    "session_id": "",
                    "should_store_message": True
                }
            }
        }
        
        # Debug: Print the payload being sent
        print("Payload:")
        print(json.dumps(payload, indent=4))

        # Send the request
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.text}
    except Exception as e:
        return {"error": str(e)}

# Test the function with a specific post_type
post_type = "carousel"  # Replace with the desired post type
output = send_metrics(post_type)
print("Langflow API Response:")
print(json.dumps(output, indent=4))
