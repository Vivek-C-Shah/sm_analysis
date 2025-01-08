from data_pipeline import DataPipeline

pipeline = DataPipeline()

print("Initializing Data Pipeline...")

collection_schema = pipeline.get_collection_schema("posts_data")
print("Collection Schema: ", collection_schema)

# collection_data = pipeline.get_collection_data("posts_data")
# print("Collection Data: ", collection_data)

# read csv  
collection_data = pipeline.read_csv('./Resources/social_media_posts.csv')

engagement_metrics = pipeline.get_engagement_metrics(collection_data)
print("Engagement Metrics: ", engagement_metrics)

engagement_metrics_by_post_type = pipeline.get_engagement_metrics_by_post_type(collection_data)
print("Engagement Metrics by Post Type: ", engagement_metrics_by_post_type)

system_prompt = pipeline.generate_system_prompt()
# print("System Prompt: ", system_prompt)

payload = pipeline.generate_payload(engagement_metrics_by_post_type, system_prompt)
# print("Payload: ", payload)

engagement_insights = pipeline.get_engagement_insights(payload)
print("Engagement Insights: ", engagement_insights)