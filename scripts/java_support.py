import os
import wget 
import tarfile
import subprocess

def setup_java():
    java_url = "https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23+9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.23_9.tar.gz"
    
    # Download & extract jdk in CWD
    base_dir = os.getcwd()
    download_path = os.path.join(base_dir, "openjdk11.tar.gz")
    extract_root = os.path.join(base_dir, "java-11")

    if not os.path.exists(download_path):
        # st.write("Downloading Java 11...") # later on write this in log
        wget.download(java_url, download_path)

    if not os.path.exists(extract_root):
        # st.write("Extracting Java 11...") # idem
        os.makedirs(extract_root, exist_ok=True)
        with tarfile.open(download_path, "r:gz") as tar:
            tar.extractall(path=extract_root)

    extracted_dirs = os.listdir(extract_root)
    full_java_dir = os.path.join(extract_root, extracted_dirs[0])

    os.environ["JAVA_HOME"] = full_java_dir
    os.environ["PATH"] = f"{full_java_dir}/bin:" + os.environ["PATH"]

    subprocess.run([f"{full_java_dir}/bin/java", "-version"], check=True)

    return full_java_dir

