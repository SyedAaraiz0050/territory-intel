from src.config import get_settings

settings = get_settings()

print("Loaded Google API Key:")
print(settings.google_maps_api_key)