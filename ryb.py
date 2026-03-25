import subprocess
import os
import time

# List of services: (Window Title, Command, Sub-Folder)
SERVICES = [
    ("API-Backend", "python -m uvicorn api:app --reload --port 8000", "."),
    ("Scheduler", "python scheduler.py", "."),
    ("Frontend-Dev", "npm run dev", "frontend")
]

def launch_services():
    # Get the directory where THIS script is saved
    root_dir = os.path.dirname(os.path.abspath(__file__))
    
    print(f"--- Launching Services from: {root_dir} ---")

    for title, command, folder in SERVICES:
        # Create the full path to the specific folder (e.g., .../your_project/frontend)
        target_dir = os.path.join(root_dir, folder)
        
        print(f"Starting {title}...")

        # 'start' creates the window
        # 'cmd /k' executes the string and stays open
        # We explicitly 'cd' to the target_dir first, then run the command
        full_command = f'start "{title}" cmd /k "cd /d {target_dir} && {command}"'
        
        subprocess.Popen(full_command, shell=True)
        
        # Brief pause to let Windows initialize the window before the next one pops up
        time.sleep(1)

    print("\nCheck your taskbar for three new terminal windows.")

if __name__ == "__main__":
    launch_services()


    # call it hub not bay

# get accurate names (same one from zapmap, and put that on the site)
# looking at 24 hour charging trends
# which sites sustain traffic
# which sites have got bit holes
# tracking the relevant sites in the national inventory
# doint it for a long enough period that we can see trends and patterns
# going back in, seeing how theyre doing well/badly
# second row showing updated stats for filtered data
# alsoi want to support by providers



#measure time a bay is in maximum capacity daily
#sort database out -> ensure old data is still being stored so it can be compared (perhaps a table for each charger?)
#fix scraping from individual charger points




#save only plotted datapoints into /ouput so we only save history + picked information
