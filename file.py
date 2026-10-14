import subprocess

def gfal_ls(path):
    try:
        result = subprocess.check_output(
            ["gfal-ls", path],
            text=True
        )
        return [line.strip() for line in result.split("\n") if line.strip()]
    except subprocess.CalledProcessError:
        return []


def list_files_recursive(base_path):
    files = []

    def explore(path):
        entries = gfal_ls(path)

        for entry in entries:
            full_path = path.rstrip("/") + "/" + entry

            if entry.endswith(".root"):
                files.append(full_path)
            else:
                sub_entries = gfal_ls(full_path)
                if sub_entries:
                    explore(full_path)

    explore(base_path)
    return files


"""choose your dataset WARNING for some sample need to investigate the version of the dataset and change the path accordingly"""

base_path = "davs://sbgdcache.in2p3.fr/cms/phedex//store/user/xmadre/SIM_MC_Part/DYJetsToLL_0J_MLL_400_800_TuneCP5_13TeV-amcatnloFXFX-pythia8/" 

files_davs = list_files_recursive(base_path)

files_root = [
    f.replace("davs://sbgdcache.in2p3.fr", "root://sbgdcache.in2p3.fr/")
    for f in files_davs
]


with open("files.txt", "w") as f:
    for file in files_root:
        f.write(file + "\n")

print(f"{len(files_root)} file scrib in files.txt")