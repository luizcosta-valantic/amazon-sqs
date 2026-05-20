using OutSystems.ExternalLibraries.SDK;
using psn.PH.Structures;

namespace psn.PH
{
    /// <summary>
    /// Connect with AWS SQS to send, store, and receive messages between software components.
    /// </summary>
    [OSInterface(Description = "Connect with AWS SQS to send, store, and receive messages between software components.", Name = "AWS_SQS_ExternalLogic_UR", IconResourceName = "psn.PH.AWS_SQS_ExtIcon.png")]
    public interface IAWS_SQS_Ext
    {
        [OSAction(Description = "Creates a new standard or FIFO queue.", ReturnName = "QueueURL")]
        public string Create_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueName, bool IsFiFo, bool UseContentBasedDeduplication);
        [OSAction(Description = "Delete a queue by URL.", ReturnName = "IsSuccessful")]
        public bool Delete_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueUrl);
        [OSAction(Description = "Delete a queue by name.", ReturnName = "IsSuccessful")]
        public bool Delete_Queue_ByName_Ext(AWS_Authenticationinfo authInfo, string QueueName);
        [OSAction(Description = "Get a Queue URL by name.", ReturnName = "QueueURL")]
        public string Get_QueueUrl_Ext(AWS_Authenticationinfo authInfo, string QueueName);
        [OSAction(Description = "Deletes the specified message from the specified queue. You specify the message by using the message's receipt handle and not the MessageId you receive when you send the message.", ReturnName = "IsSuccessful")]
        public bool Delete_Message_Ext(AWS_Authenticationinfo authInfo, string QueueName, string ReceiptHandle, int TimeoutInSeconds);
        [OSAction(Description = "Delivers a message to the specified queue.", ReturnName = "MessageId")]
        public string Send_Message_Ext(AWS_Authenticationinfo authInfo, string QueueUrl, string MessageBody, string MessageDeduplicationId, string MessageGroupId, List<SQS_MessageAttribute> MessageAttributes);
        [OSAction(Description = "Retrieves one or more messages (up to 10), from the specified queue.", ReturnName = "Messages")]
        public List<SQS_Message> Receive_Message_Ext(AWS_Authenticationinfo authInfo, string QueueUrl, int MaxNumberOfMessages, List<SQS_MessageAttribute> MessageAttributeNames, int WaitTimeSeconds);
        [OSAction(Description = "Deletes the messages in a queue specified by the QueueURL parameter.", ReturnName = "IsSuccessful")]
        public bool Purge_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueUrl);

        /// <summary>
        /// Retrieve unique build information of this custom library.
        /// </summary>
        [OSAction(Description = "Get unique build information of this custom library.", ReturnName = "buildInfo")]
        public string GetBuildInfo_Ext();

    }
}