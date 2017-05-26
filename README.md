# Azure-ADLS-Blob-Data-Copy
This allows you to place files in a queue that need to be copied between two Azure Data Lake accounts.  The current example shows copying files from a ADLS account in one subscription to another subscription (you can modify for blob copy).  There are other Azure technolgioes that can do this like Azure Data Factory, ADLCopy, AzCopy and Hadoop Distcp, but sometimes things like VNETs and permissions get in the way.  This provides a more manually move the files.  The perferred apporach for millions of files is to use a HDInsight cluster that can see both ADLS or Blob accounts for the transfer, but that is not always possible.  This apporach uses a VM Scale Set that you can scale to many computers to increase your rate of copy.

1.	Update Data Lake Copy - Queue.ps1 values at the top
2.	Run this by hand (currently this is recursively reading a data lake directory since I am copying between two subscriptions ADLS to ADLS)
3.	Update Data Lake Copy - Process Queue.ps1 values at the top
4.	Upload "Update Data Lake Copy - Process Queue.ps1" to blob and get a SAS token for the below ARM template (use http://storageexplorer.com/ and right click to get a SAS token)
5.	Create VM ScaleSet in the Azure portal.
6.  At the last step export the template
7.  Modify the template ARM template adding the below:
    NOTE: The below is just running one copy per computer "commandToExecute" this can be chagned to spawn off many.  Please keep in mind you do not want to overfill the Azure hard disk, so pick a VM size with a large enough ephemeral disk.
    
          "extensionProfile": {
            "extensions": [
              {
                "name": "customScript",
                "properties": {
                  "publisher": "Microsoft.Compute",
                  "settings": {
                    "fileUris": [
                      "<<REMOVED>> e.g. https://mmystorage.blob.core.windows.net/Data Lake Copy - Process Queue.ps1"
                    ]
                  },
                  "typeHandlerVersion": "1.8",
                  "autoUpgradeMinorVersion": true,
                  "protectedSettings": {
                    "commandToExecute": "powershell -ExecutionPolicy Unrestricted -File Data Lake Copy - Process Queue.ps1"
                  },
                  "type": "CustomScriptExtension"
                }
              }

