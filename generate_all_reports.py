#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Master Generator
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026

Generates all 24 industry-specific proof reports for LinkedIn portfolio.
"""

import subprocess
import sys
import os
from datetime import datetime

BASE_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package"

def run_generator(name, script):
    """Run a report generator script"""
    print(f"\n{'='*60}")
    print(f"Running {name} Reports...")
    print('='*60)

    script_path = os.path.join(BASE_DIR, script)
    result = subprocess.run([sys.executable, script_path], capture_output=False)
    return result.returncode == 0

def main():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║   CMS LinkedIn Proof Package Generator                       ║
    ║   Author: Mboya Jeffers (MboyaJeffers9@gmail.com)           ║
    ║                                                              ║
    ║   Generating 24 reports across 6 industries:                 ║
    ║   • Finance (4 reports)                                      ║
    ║   • Solar (4 reports)                                        ║
    ║   • Weather (4 reports)                                      ║
    ║   • Sports (4 reports)                                       ║
    ║   • Betting (4 reports)                                      ║
    ║   • Compliance (4 reports)                                   ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    start_time = datetime.now()

    generators = [
        ("Finance", "generate_finance_reports.py"),
        ("Solar", "generate_solar_reports.py"),
        ("Weather", "generate_weather_reports.py"),
        ("Sports", "generate_sports_reports.py"),
        ("Betting", "generate_betting_reports.py"),
        ("Compliance", "generate_compliance_reports.py"),
    ]

    results = {}
    for name, script in generators:
        success = run_generator(name, script)
        results[name] = "✓" if success else "✗"

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"""

    ╔══════════════════════════════════════════════════════════════╗
    ║                    GENERATION COMPLETE                       ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║   Results:                                                   ║
    ║   {results.get('Finance', '?')} Finance      {results.get('Solar', '?')} Solar        {results.get('Weather', '?')} Weather      ║
    ║   {results.get('Sports', '?')} Sports       {results.get('Betting', '?')} Betting      {results.get('Compliance', '?')} Compliance   ║
    ║                                                              ║
    ║   Output: {BASE_DIR}
    ║                                                              ║
    ║   Duration: {duration:.1f} seconds                                     ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝

    Next Steps:
    1. Review reports in each industry folder
    2. Take screenshots for LinkedIn carousel posts
    3. Upload to cleanmetricsstudios.com as portfolio pieces
    4. Share with #dataengineering #datascience hashtags
    """)

if __name__ == "__main__":
    main()
