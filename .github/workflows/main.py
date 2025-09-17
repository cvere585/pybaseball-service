import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pybaseball import team_batting, team_pitching, batting_stats, pitching_stats
import pandas as pd
from datetime import datetime
from functools import lru_cache

app = FastAPI()

# Cache results to avoid re-fetching data too frequently
@lru_cache(maxsize=128)
def get_team_batting_cached(season: int):
    return team_batting(season)

@lru_cache(maxsize=128)
def get_team_pitching_cached(season: int):
    return team_pitching(season)
    
@lru_cache(maxsize=128)
def get_batting_stats_cached(season: int):
    return batting_stats(season)

@lru_cache(maxsize=128)
def get_pitching_stats_cached(season: int):
    return pitching_stats(season)

def clean_data(df):
    """Convert numpy types to native Python types for JSON serialization."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Convert integer-like floats to int, and others to float, handling NaNs
            if pd.api.types.is_float_dtype(df[col]):
                # Check if all non-NaN values are integers
                if (df[col].dropna() == df[col].dropna().astype(int)).all():
                    df[col] = df[col].astype('Int64') # Use nullable integer type
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Fill NaNs *after* type conversion for clean JSON output
    return df.fillna(0).to_dict(orient='records')

@app.get("/")
def read_root():
    return {"status": "PyBaseball service is running"}

@app.get("/stats")
async def get_stats(dataType: str, season: int = None):
    if not season:
        season = datetime.now().year
        
    try:
        if dataType == "team_stats":
            batting = get_team_batting_cached(season)
            pitching = get_team_pitching_cached(season)
            batting = batting.rename(columns={"SO": "SO_batting", "BB": "BB_batting"})
            pitching = pitching.rename(columns={"SO": "SO_pitching", "BB": "BB_pitching"})
            merged = pd.merge(batting, pitching, on="Team", how="outer")
            data = clean_data(merged)
            
        elif dataType == "player_batting":
            stats = get_batting_stats_cached(season)
            stats = stats[stats['AB'] >= 100]
            data = clean_data(stats)

        elif dataType == "player_pitching":
            stats = get_pitching_stats_cached(season)
            stats = stats[stats['IP'] >= 40]
            data = clean_data(stats)
            
        else:
            raise HTTPException(status_code=400, detail="Invalid dataType")
            
        return JSONResponse(content={"success": True, "data": data})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Replit will use the .replit file to run this, but this is good for local testing
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
