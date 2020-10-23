## LIBRARIES USED
import os
import csv
import itertools
import pandas as pd

## FUNCTIONS

def delInstanceBreaks(inPath, outPath, encoding="latin1", sep="|"):
    """Delete unwanted linebreaks within cells of the same instance.
    
    Keyword arguments:
    path -- String with the file path.
    newPath -- String with the directory of the output file.
    """
    ## Delete unwanted linebreaks
    with open(inPath, encoding=encoding, newline=None, mode="rt") as rcc_in:
        with open(outPath, encoding=encoding, newline=None, mode="wt") as rcc_out:
            # First row (names) is copied directly
            iterLineas = iter(rcc_in)
            rcc_out.write(next(iterLineas))
            # Read following lines in pairs and replace
            for linea1, linea2 in itertools.zip_longest(*[iterLineas]*2):
                if not linea2: break
                else:  
                    lineasJuntas = linea1+linea2
                    stringToReplace = "\n"+sep
                    replaceWith = ""
                    rcc_out.write(lineasJuntas.replace(stringToReplace, replaceWith))
    ## Succes message
    outDir = os.path.dirname(outPath)
    outBasename = os.path.basename(outPath)
    print("File '{}' saved at '{}'.".format(outBasename, outDir))

    
def txtToFeather(inPath, outPath, idCol="DOCUMENTO"):
    """Parses txt file and converts to feather format."""
    print("Parsing text file...")
    data = pd.read_csv(inPath, encoding="latin1", sep="|", quotechar='"', doublequote=False, 
                       engine="python", dtype="object", error_bad_lines=False)
    print("Text file parsed. Processing...")
    data.reset_index(drop=True, inplace=True)
    data.to_feather(path=outPath)
    ## Succes message
    print("File saved at '{}'.".format(outPath))
    print("Sample data:")
    print(data.head())


from google.cloud import storage

def upload_blob(project_name, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project=project_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    ) 