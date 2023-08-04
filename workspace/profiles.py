import os
from serra.profile import get_serra_profile

def create_databrickscfg():
    serra_profile = get_serra_profile()

    # Get the user's home directory
    home_directory = os.path.expanduser("~")

    # Generate the .databrickscfg content
    databrickscfg = serra_profile.get_databrickscfg()

    # Write the .databrickscfg file
    with open(os.path.join(home_directory, ".databrickscfg"), "w") as f:
        f.write(databrickscfg)

if __name__ == "__main__":
    create_databrickscfg()