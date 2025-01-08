from langflow.load import run_flow_from_json
import pandas as pd

# Path to Langflow Workflow JSON
FLOW_PATH = "Basic Prompting.json"

# Langflow Workflow Configuration
TWEAKS = {
    "ChatInput-I77JO": {
        "background_color": "",
        "chat_icon": "",
        "files": "",
        "input_value": "Hello",
        "sender": "User",
        "sender_name": "User",
        "session_id": "",
        "should_store_message": True,
        "text_color": ""
    },
    "Prompt-8eRmY": {
        "template": """
Analyze the following data for social media post type: "{post_type}".

Metrics:
- Average Likes: {avg_likes}
- Average Shares: {avg_shares}
- Average Comments: {avg_comments}
- Engagement Rate: {engagement_rate}
- Like-to-Comment Ratio: {like_to_comment_ratio}
- Share-to-Like Ratio: {share_to_like_ratio}

Based on these metrics, provide actionable insights to improve engagement and performance.
"""
    },
    "OpenAIModel-MIPkw": {
        "api_key": "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",  # Replace with your OpenAI API Key
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

# Load metrics dataset
metrics_df = pd.read_csv("final_metrics.csv")

# Function to retrieve metrics for a specific post type
def get_metrics_for_prompt(post_type):
    row = metrics_df[metrics_df["post_type"] == post_type].iloc[0]
    return {
        "post_type": row["post_type"],
        "avg_likes": row["avg_likes"],
        "avg_shares": row["avg_shares"],
        "avg_comments": row["avg_comments"],
        "engagement_rate": row["engagement_rate"],
        "like_to_comment_ratio": row["like_to_comment_ratio"],
        "share_to_like_ratio": row["share_to_like_ratio"]
    }

# Function to generate insights using Langflow
def generate_insights(post_type):
    # Fetch metrics for the given post type
    metrics = get_metrics_for_prompt(post_type)
    
    # Format the prompt with metrics
    TWEAKS["Prompt-8eRmY"]["template"] = TWEAKS["Prompt-8eRmY"]["template"].format(**metrics)
    
    # Run the Langflow workflow
    result = run_flow_from_json(
        flow=FLOW_PATH,
        session_id="unique_session_id",  # Optionally use a session ID
        fallback_to_env_vars=True,  # Use environment variables if needed
        tweaks=TWEAKS
    )
    
    # Return GPT response
    return result["response"]

# Test the workflow with a specific post type
if __name__ == "__main__":
    post_type = "carousel"  # Change post type as needed
    insights = generate_insights(post_type)
    print(f"Generated Insights for {post_type}:")
    print(insights)
