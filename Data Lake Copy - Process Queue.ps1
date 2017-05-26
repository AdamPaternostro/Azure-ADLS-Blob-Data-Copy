# You can run this many times on the same server and should
# You will need a Service Principle that has access to both data lake accounts
# This is currently copying data lake to data lake (you could change the source/dest to blob)
# Note: Make sure if you are downloading to the Azure D drive (ephemeral disk) that you do not run out of space.

# Set these values
$StorageAccountKey  = "<<REMOVED>> e.g. 00000000-0000-0000-0000-000000000000"
$StorageAccountName = "<<REMOVED>> e.g. mystorageaccount"
$QueueName          = "adlcopy"

$SourceSubscriptionId  = "<<REMOVED>> e.g. 00000000-0000-0000-0000-000000000000"
$SourceDataLakeAccount = "<<REMOVED>> e.g. sampleazuredatalakestore"

$DestinationSubscriptionId  = "<<REMOVED>>"
$DestinationDataLakeAccount = "<<REMOVED>> e.g. destazuredatalakestore"

# Service princple who has been granted ADLS access to both data lakes
$CLIENT_KEY      = "<<REMOVED>>"
$CLIENT_ID       = "<<REMOVED>>"
$TENANT_ENDPOINT = "<<REMOVED>>"


# In Azure this should be the D Drive or mounted F Drive
$TemporaryDirectory = "D:\ADLCopy-" + [GUID]::NewGuid().ToString() + "\"
$TemporaryFile = $TemporaryDirectory + "temp.txt"

# Login with Service Principle
$secpasswd = ConvertTo-SecureString $CLIENT_KEY -AsPlainText -Force
$mycreds   = New-Object System.Management.Automation.PSCredential ($CLIENT_ID, $secpasswd)
Login-AzureRmAccount -ServicePrincipal -Tenant $TENANT_ENDPOINT -Credential $mycreds
Select-AzureRmSubscription -SubscriptionId $SourceSubscriptionId

# Create a working directory
New-Item -ItemType Directory -Force -Path $TemporaryDirectory | Out-Null

#Define the storage account and context.
$Ctx = New-AzureStorageContext –StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey

# Get queue reference
$Queue = Get-AzureStorageQueue –Name $QueueName –Context $Ctx

# You would need to set this based upon the File size (1 hour, 30 min, etc...)
$InvisibleTimeout = [System.TimeSpan]::FromHours(3)

#Get the message object from the queue.
$QueueMessageFromQueue = $Queue.CloudQueue.GetMessage($InvisibleTimeout)


while ($QueueMessageFromQueue -ne $null)
    {
    ####################
    # Download Source
    ####################
    Select-AzureRmSubscription -SubscriptionId $SourceSubscriptionId

    $write =  "BEGIN Download: " + $QueueMessageFromQueue.AsString
    Write-Output $write
    Export-AzureRmDataLakeStoreItem -AccountName $SourceDataLakeAccount     -Path $QueueMessageFromQueue.AsString -Destination $TemporaryFile -Force
    $write =  "END Download:   " + $QueueMessageFromQueue.AsString
    Write-Output $write


    ####################
    # Upload Destination
    ####################
    Select-AzureRmSubscription -SubscriptionId $DestinationSubscriptionId

    $write =  "BEGIN Upload: " + $QueueMessageFromQueue.AsString
    Write-Output $write
    Import-AzureRmDataLakeStoreItem -AccountName $DestinationDataLakeAccount -Path $localFolder$TemporaryFile -Destination $QueueMessageFromQueue.AsString -Force
    $write =  "END   Upload: " + $QueueMessageFromQueue.AsString
    Write-Output $write

 
    Remove-Item -Path $TemporaryFile -Force -ErrorAction SilentlyContinue


    ####################
    # Queue Logic
    ####################
    #Delete the message.
    $Queue.CloudQueue.DeleteMessage($QueueMessageFromQueue)

    #Get the message object from the queue.
    $QueueMessageFromQueue = $Queue.CloudQueue.GetMessage($InvisibleTimeout)
    } # whlie

# Create a working directory
Remove-Item  -Force -Path $TemporaryDirectory | Out-Null

Write-Output "Done."