using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace BatchDotNetQuickstart
{
    public class Program
    {
        // Update the Batch and Storage account credential strings below with the values unique to your accounts.
        // These are used when constructing connection strings for the Batch and Storage client objects.

        // Batch account credentials
        private const string BatchAccountName = "";
        private const string BatchAccountKey = "";
        private const string BatchAccountUrl = "";

        // Storage account credentials
        private const string StorageAccountName = "";
        private const string StorageAccountKey = "";

        // Batch resource settings
        private const string PoolId = "DotNetQuickstartPool";
        private const string JobId = "DotNetQuickstartJob";
        private const int PoolNodeCount = 2;
        private const string PoolVMSize = "STANDARD_A1_v2";

        static void Main()
        {
            if (String.IsNullOrEmpty(BatchAccountName) ||
                String.IsNullOrEmpty(BatchAccountKey) ||
                String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) ||
                String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            try
            {
                Console.WriteLine("Sample start: {0}", DateTime.Now);
                Console.WriteLine();
                var timer = new Stopwatch();
                timer.Start();

                // Create the blob client, for use in obtaining references to blob storage containers
                var blobServiceClient = GetBlobServiceClient(StorageAccountName, StorageAccountKey);

                // Use the blob client to create the input container in Azure Storage 
                const string inputContainerName = "input";
                var containerClient = blobServiceClient.GetBlobContainerClient(inputContainerName);
                containerClient.CreateIfNotExistsAsync().Wait();

                // The collection of data files that are to be processed by the tasks
                List<string> inputFilePaths = new()
                {
                    "taskdata0.txt",
                    "taskdata1.txt",
                    "taskdata2.txt"
                };

                // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
                // executed on the compute nodes within the pool.
                var inputFiles = new List<ResourceFile>();

                foreach (var filePath in inputFilePaths)
                {
                    inputFiles.Add(UploadFileToContainer(containerClient, inputContainerName, filePath));
                }

                // Get a Batch client using account creds
                var cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

                using BatchClient batchClient = BatchClient.Open(cred);
                Console.WriteLine("Creating pool [{0}]...", PoolId);

                // Create a Windows Server image, VM configuration, Batch pool
                ImageReference imageReference = CreateImageReference();
                VirtualMachineConfiguration vmConfiguration = CreateVirtualMachineConfiguration(imageReference);
                CreateBatchPool(batchClient, vmConfiguration);

                // Create a Batch job
                Console.WriteLine("Creating job [{0}]...", JobId);

                try
                {
                    CloudJob job = batchClient.JobOperations.CreateJob();
                    job.Id = JobId;
                    job.PoolInformation = new PoolInformation { PoolId = PoolId };
                    job.Commit();
                }
                catch (BatchException be)
                {
                    // Accept the specific error code JobExists as that is expected if the job already exists
                    if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                    {
                        Console.WriteLine("The job {0} already existed when we tried to create it", JobId);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }

                // Create a collection to hold the tasks that we'll be adding to the job
                Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, JobId);
                var tasks = new List<CloudTask>();

                // Create each of the tasks to process one of the input files. 
                for (int i = 0; i < inputFiles.Count; i++)
                {
                    string taskId = string.Format("Task{0}", i);
                    string inputFilename = inputFiles[i].FilePath;
                    string taskCommandLine = string.Format("cmd /c type {0}", inputFilename);

                    var task = new CloudTask(taskId, taskCommandLine)
                    {
                        ResourceFiles = new List<ResourceFile> { inputFiles[i] }
                    };
                    tasks.Add(task);
                }

                // Add all tasks to the job.
                batchClient.JobOperations.AddTask(JobId, tasks);

                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
                TimeSpan timeout = TimeSpan.FromMinutes(30);
                Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                IEnumerable<CloudTask> addedTasks = batchClient.JobOperations.ListTasks(JobId);
                batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);
                Console.WriteLine("All tasks reached state Completed.");

                // Print task output
                Console.WriteLine();
                Console.WriteLine("Printing task output...");

                IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(JobId);
                foreach (CloudTask task in completedtasks)
                {
                    string nodeId = String.Format(task.ComputeNodeInformation.ComputeNodeId);
                    Console.WriteLine("Task: {0}", task.Id);
                    Console.WriteLine("Node: {0}", nodeId);
                    Console.WriteLine("Standard out:");
                    Console.WriteLine(task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
                }

                // Print out some timing info
                timer.Stop();
                Console.WriteLine();
                Console.WriteLine("Sample end: {0}", DateTime.Now);
                Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                // Clean up Storage resources
                containerClient.DeleteIfExistsAsync().Wait();
                Console.WriteLine("Container [{0}] deleted.", inputContainerName);

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.JobOperations.DeleteJob(JobId);
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.PoolOperations.DeletePool(PoolId);
                }
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        private static void CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration)
        {
            try
            {
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: PoolNodeCount,
                    virtualMachineSize: PoolVMSize,
                    virtualMachineConfiguration: vmConfiguration);

                pool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.windows amd64");
        }

        private static ImageReference CreateImageReference()
        {
            return new ImageReference(
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: "2016-datacenter-smalldisk",
                version: "latest");
        }

        /// <summary>
        /// Creates a blob client
        /// </summary>
        /// <param name="storageAccountName">The name of the Storage Account</param>
        /// <param name="storageAccountKey">The key of the Storage Account</param>
        /// <returns></returns>
        private static BlobServiceClient GetBlobServiceClient(string storageAccountName, string storageAccountKey)
        {
            var sharedKeyCredential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
            string blobUri = "https://" + storageAccountName + ".blob.core.windows.net";

            var blobServiceClient = new BlobServiceClient(new Uri(blobUri), sharedKeyCredential);
            return blobServiceClient;
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="containerClient">A <see cref="BlobContainerClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile instance representing the file within blob storage.</returns>
        private static ResourceFile UploadFileToContainer(BlobContainerClient containerClient, string containerName, string filePath, string storedPolicyName = null)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);
            string blobName = Path.GetFileName(filePath);
            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            var blobClient = containerClient.GetBlobClient(blobName);
            blobClient.Upload(filePath, true);

            // Set the expiry time and permissions for the blob shared access signature. 
            // In this case, no start time is specified, so the shared access signature 
            // becomes valid immediately
            // Check whether this BlobContainerClient object has been authorized with Shared Key.
            try
                if (blobClient.CanGenerateSasUri)
                {
                    // Create a SAS token
                    var sasBuilder = new BlobSasBuilder()
                    {
                        BlobContainerName = containerClient.Name,
                        BlobName = blobClient.Name,
                        Resource = "b"
                    };
    
                    if (storedPolicyName == null)
                    {
                        sasBuilder.ExpiresOn = DateTimeOffset.UtcNow.AddHours(1);
                        sasBuilder.SetPermissions(BlobContainerSasPermissions.Read);
                    }
                    else
                    {
                        sasBuilder.Identifier = storedPolicyName;
                    }
    
                    var sasUri = blobClient.GenerateSasUri(sasBuilder).ToString();
                    return ResourceFile.FromUrl(sasUri, filePath);
                }
                else
                {
                    throw new InvalidOperationException("BlobClient must be authorized with shared key credentials to create a service SAS.");
                }
            catch (Exception ex)
                {
                    "Console.WriteLine($"{ex.Message}");
                }
        }
    }
}
