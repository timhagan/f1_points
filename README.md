# 🏁 F1 Points Tracker
A modern, automated Formula 1 Fantasy standings tracker that provides driver and constructor fantasy points for the current Formula 1 season.

# 🌟 Features
- Fantasy Points Scored: Up-to-date driver and constructor championship points from the most recent race
- Automated Updates: GitHub Actions workflows automatically fetch new data after each race
- Clean Interface: Responsive web design with tabbed navigation
- Custom Scoring: Includes additional points for pole positions and places gained
- Historical Data: Tracks points progression throughout the season (Coming Soon!)

# 🚀 Live Site
Visit the live standings at: timhagan.github.io/f1_points

# 📊 What's Tracked
- Driver Points
- Race finishing positions
- Sprint race results
- Pole position bonuses (10 points)
- Places gained bonuses (points for overtaking from grid position)
- Constructor Points
- Combined team points from both drivers
- Sprint and race weekend totals
- Season progression tracking

# 🛠️ How It Works
- Data Pipeline
  - Event Schedule: Refreshed automatically in-race pipeline runs and via annual pre-season sync
  - Race Results: Automatically collected after each Grand Prix
  - Points Calculation: Custom scoring system applied to raw results
  - Web Update: New standings automatically pushed to GitHub Pages
- Automation Schedule
  - 📅 Annual: January 1st - Fetch F1 event schedule
  - 🏁 Weekly: Every Monday - Process race results from weekend
  - 🔄 Manual: On-demand triggers available for Sprint weekends

# 📁 Project Structure
```
f1_points/
├── .github/workflows/     # GitHub Actions automation
├── src/data_prep/         # Python data processing scripts
├── data/                  # Generated CSV files with standings
├── tests/                 # Unit tests for core functions  
├── index.html             # Main web interface
└── requirements.txt       # Python dependencies
```

# 🧪 Local Development
Prerequisites
- Python 3.11+
- pip package manager
Setup
```
# Clone the repository
git clone https://github.com/yourusername/f1_points.git
cd f1_points

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # macOS/Linux

# Install dependencies  
pip install -r requirements.txt
```

Run Tests
```
# Run unit tests
pytest

# Test complete pipeline manually
python src/data_prep/get_most_recent_event_points.py
python src/data_prep/combine_event_points.py
```

Manual Data Updates
```
# Get latest race results
python src/data_prep/get_most_recent_event_points.py

# Combine all season data
python src/data_prep/combine_event_points.py
```

# 🤝 Contributing
Contributions welcome! Here's how to help:

Reporting Issues
- Found incorrect points? Open an issue with race details
- Site not loading? Include browser and error info
- Missing race data? Let us know which Grand Prix

Code Contributions
1. Fork the repository
2. Create a feature branch (git checkout -b feature/amazing-feature)
3. Make your changes and add tests
4. Run the test suite (pytest)
5. Commit changes (git commit -m 'Add amazing feature')
6. Push to branch (git push origin feature/amazing-feature)
7. Open a Pull Request

Areas for Improvement
- Historical season comparisons
- Driver/team performance analytics
- Mobile app companion

# 📋 Data Sources
[Fast_F1](https://github.com/theOehrly/Fast-F1)
- F1 API: Official Formula 1 timing and results
- Ergast API: Historical Formula 1 results

# 🔧 Technical Details
- Frontend: HTML/CSS/JavaScript with Tabulator.js tables
- Backend: Python pandas
- Automation: GitHub Actions
- Hosting: GitHub Pages
- Data Format: CSV files

# 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

# 🏎️ Acknowledgments
[Fast_F1](https://github.com/theOehrly/Fast-F1), [jolpica-f1](https://github.com/jolpica/jolpica-f1/tree/main), and Formula 1 for providing official timing data

🏁 Keep up with your fantasy team by following every lap, every point, every championship battle!
