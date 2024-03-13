# NBA Moonshot Predictor

### "Can the Moon predict Steph Curry's performance?"

Welcome to the NBA Moonshot Predictor! This project is designed to showcase skills in machine learning, data engineering, and overall Python programming while keeping it fun and interesting.

## Project Overview

The NBA Moonshot Predictor is divided into three main phases to highlight different skills in the realm of machine learning and data engineering. The ultimate goal is to create a machine learning model that can predict whether an NBA player will perform under or over their expected performance based on the night's lunar phase on the day of the game. While it's a lighthearted experiment, I hope to demonstrate my technical skills and creative thinking to apply to real world problems!

### Phase 1: Data Gathering and Ingestion ✅

The first phase of the project is complete! During this phase, I setup the database schema in PostgreSQL, consumed data from the NBA API (through the nba_api package), and parsed, transformed, and stored this data into various tables. In addition, I combined NBA arena location data found online in order to accurately gather lunar phase data from the Astronomy API for each game based on the date and location of the game.

**Key Skills Demonstrated:**
- API Consumption: Extracted data from the NBA and Astronomy APIs.
- Data Pipelines: Created scripts to fetch, transform, and store the data. (Will automate during the third phase!)
- PostgreSQL: Stored and managed the dataset in a PostgreSQL database.

All related modules and scripts for this phase are organized within the `data_pipeline` directory of the NBA-moonshot-predictor project.

### Phase 2: Building the Machine Learning Model 🚧

**Current Phase - Work in Progress**

Now, I'm diving into the heart of our project - the machine learning model. This phase is all about pulling our curated data from the database, cleaning it, engineering features, and performing exploratory data analysis (EDA). I will be testing various machine learning models to find the one that best captures the lunar influence on player performance.

**What I'm Doing:**
- Data Cleaning: Preparing the dataset for modeling.
- Feature Engineering: Crafting informative features that capture the essence of lunar phases and player performance.
- Model Testing: Experimenting with different machine learning algorithms to find our MVP (Most Valuable Predictor).

### Phase 3: Deployment and Interaction 🌑➡️🌕

The final phase of the project involves deploying the model to a web platform. Here, I'll provide an interactive experience for visitors, allowing them to view the model's performance and explore the lunar effects on upcoming games. This phase is still on the drawing board, but it promises to be the most exciting part of our lunar analytics odyssey.

**Future Skills to Be Showcased:**
- Web Development: Bringing our model to the internet.
- Interactive Predictions: Allowing users to engage with our model in real-time.
- Deployment: Making our project accessible to a wider audience.

## Resources

The following resources have been utilized to gather the necessary data for the model:

- [Astronomy API](https://astronomyapi.com/) for lunar phase data.
- [NBA API](https://github.com/swar/nba_api) for player performance and game statistics.
- [NBA Data Spreadsheet](https://docs.google.com/spreadsheets/d/1p0R5qqR7XjoRG2mR5E1D_trlygHSqMOUdMgMpzq0gjU/htmlview) for NBA arena location data.

---

**Note:** This project is currently in phase 2 and is actively being developed. Keep an eye on this space for updates and the eventual launch of the predictive model.

Feel free to explore the code, suggest improvements, or reach out if you have any questions or ideas!
