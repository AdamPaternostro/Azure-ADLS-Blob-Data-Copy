# Run this once to queue all your files 
# This will loop through a data lake and insert all the files into a queue
# The storage account (for the queue) must exist and you need the key
# Note: You can change this to loop through a blob account 
# Note: This in not a good apporach if you have millions and millions of files, use a HDInsight cluster with distcp instead
#       See: https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-copy-data-wasb-distcp 
#            hadoop distcp -Dmapred.listing.split.ratio=1 -strategy dynamic -m 400 -bandwidth 2048 wasb://mystorage.blob.core.windows.net/source adl://mylake.azuredatalakestore.net/dest/ >> transfer.log

# Set these values
$SubscriptionId        = "<<REMOVED>> e.g. 00000000-0000-0000-0000-000000000000"
$StorageAccountKey     = "<<REMOVED>>"
$StorageAccountName    = "<<REMOVED>> e.g. mystorageaccount"
$SourceDataLakeAccount = "<<REMOVED>> e.g. myazuredatalakestore"
$SourceDataLakePath    = "<<REMOVED>> e.g. /hadoopfiles"
$QueueName             = "adlcopy"


Login-AzureRmAccount
Select-AzureRmSubscription -SubscriptionId $SubscriptionId


#Define the storage account and context.
$Ctx = New-AzureStorageContext –StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey

# Get queue reference
$created = $false

while ($created -eq $false)
    {
    $Queue = Get-AzureStorageQueue –Name $QueueName –Context $Ctx -ErrorAction Ignore
    if ($Queue -ne $null)
        {
            $created = $true
        }
    else
        {
        # Create queue (ignore if exists)
        New-AzureStorageQueue –Name $QueueName -Context $Ctx -ErrorAction SilentlyContinue

        # Wait for it to be created
        Write-Output "Waiting for queue to be created"
        Start-Sleep -s 10
        }
    } # while


# Insert all the files to copy
copyacls -Account $SourceDataLakeAccount -Path $SourceDataLakePath -Queue $Queue

Write-Output "Done."


# Inserts all the files names in the source data lake path into the queue
function copyacls
{
    param
    (
        [string] $Account,
        [string] $Path,
        [object] $Queue
    )
    
    $itemList = Get-AzureRMDataLakeStoreChildItem -Account $Account -Path $Path;

    foreach($item in $itemList)
    {
        $pathToSet = Join-Path -Path $Path -ChildPath $item.PathSuffix;
        $pathToSet = $pathToSet.Replace("\", "/");
        
        if ($item.Type -ieq "FILE")
        {
            # Create message to insert into queue
            $QueueMessage = New-Object -TypeName Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage -ArgumentList $pathToSet

            #Add a new message to the queue.
            $Queue.CloudQueue.AddMessage($QueueMessage)

            $write =  "Adding Queue Message: " + $pathToSet
            Write-Output $write
        }

        elseif ($item.Type -ieq "DIRECTORY")
        {
            # Nothing to copy, just recurse this directory
            copyacls -Account $Account -Path $pathToSet -Queue $Queue
        }

        else
        {
            throw "Invalid path type of: $($item.Type). Valid types are 'DIRECTORY' and 'FILE'"
        }
    }
} # copyacls