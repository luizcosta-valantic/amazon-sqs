using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Linq;
using psn.PH.Structures;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace psn.PH
{
    public class AWS_SQS_Ext : IAWS_SQS_Ext
    {
        static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private long getCurrentTimeInMillis()
        {
            long ms = (long)(DateTime.UtcNow - epoch).TotalMilliseconds;
            long result = ms / 1000;
            return result;
        }
        private AmazonSQSClient getClient(AWS_Authenticationinfo authInfo)
        {
            AmazonSQSClient client = new AmazonSQSClient(authInfo.AccessKeyId.Trim(), authInfo.SecretAccessKey.Trim(), RegionEndpoint.GetBySystemName(authInfo.Region.Trim()));
            return client;
        }

        private async Task<CreateQueueResponse> Create_Queue(AmazonSQSClient client, CreateQueueRequest request)
        {
            var response = await client.CreateQueueAsync(request);
            return response;
        }
        public string Create_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueName, bool IsFiFo, bool UseContentBasedDeduplication)
        {
            AmazonSQSClient client = getClient(authInfo);
            var attrs = new Dictionary<string, string>();
            var request = new CreateQueueRequest
            {
                Attributes = attrs,
                QueueName = QueueName
            };
            if (IsFiFo)
            {
                attrs.Add(QueueAttributeName.FifoQueue, IsFiFo.ToString());
                attrs.Add(QueueAttributeName.ContentBasedDeduplication, UseContentBasedDeduplication.ToString());

                // Update the name if it is not correct for a FIFO queue.
                if (!QueueName.EndsWith(".fifo"))
                {
                    request.QueueName = QueueName + ".fifo";
                }

                // Add an attribute for a FIFO queue.
                request.Attributes.Remove(QueueAttributeName.FifoQueue);
                request.Attributes.Add(
                    QueueAttributeName.FifoQueue, "true");
            }
            else
            {
                request.QueueName = QueueName;
                request.Attributes.Clear();
                request.Attributes.Add("DelaySeconds", "60");
                request.Attributes.Add("MessageRetentionPeriod", "86400");
            }
            var createResponse = Create_Queue(client, request);
            return createResponse.Result.QueueUrl;
        }
        private async Task<bool> Delete_Queue(AmazonSQSClient client, string QueueUrl)
        {
            var deleteResponse = await client.DeleteQueueAsync(
                new DeleteQueueRequest()
                {
                    QueueUrl = QueueUrl,
                });
            return deleteResponse.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }
        public bool Delete_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueUrl)
        {
            AmazonSQSClient client = getClient(authInfo);
            var response = Delete_Queue(client, QueueUrl);
            return response.Result;
        }

        public bool Delete_Queue_ByName_Ext(AWS_Authenticationinfo authInfo, string QueueName)
        {
            AmazonSQSClient client = getClient(authInfo);
            var QueueUrl = Get_QueueUrl(client, QueueName);
            return Delete_Queue_Ext(authInfo, QueueUrl.Result);
        }
        private async Task<string> Get_QueueUrl(AmazonSQSClient client, string QueueName)
        {
            var response = await client.GetQueueUrlAsync(QueueName);
            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                return response.QueueUrl;
            }
            return string.Empty;
        }
        public string Get_QueueUrl_Ext(AWS_Authenticationinfo authInfo, string QueueName)
        {
            AmazonSQSClient client = getClient(authInfo);
            var response = Get_QueueUrl(client, QueueName);
            return response.Result;
        }

        private async Task<bool> Delete_Message(AmazonSQSClient client, string QueueName, string ReceiptHandle)
        {
            var response = Get_QueueUrl(client, QueueName);
            string queueUrl = response.Result;
            var deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = queueUrl,
                ReceiptHandle = ReceiptHandle,
            };

            var delresponse = await client.DeleteMessageAsync(deleteMessageRequest);
            return delresponse.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }
        public bool Delete_Message_Ext(AWS_Authenticationinfo authInfo, string QueueName, string ReceiptHandle)
        {
            AmazonSQSClient client = getClient(authInfo);
            var response = Delete_Message(client, QueueName, ReceiptHandle);
            return response.Result;
        }

        private async Task<string> Send_Message(AmazonSQSClient client, string QueueUrl, string MessageBody, string MessageDeduplicationId, string MessageGroupId, List<SQS_MessageAttribute> MessageAttributes)
        {
            Dictionary<string, MessageAttributeValue> msgAttributes = new Dictionary<string, MessageAttributeValue>();
            foreach (var mattr in MessageAttributes)
            {
                MessageAttributeValue mav;
                // NOTE: only support these two data type. Does not support binary
                if (mattr.DataType.Equals("String") || mattr.DataType.Equals("Number"))
                {
                    mav = new MessageAttributeValue()
                    {
                        StringValue = mattr.Value,
                    };
                    msgAttributes.Add(mattr.Name, mav);
                }
            }

            var sendMessageRequest = new SendMessageRequest
            {
                MessageAttributes = msgAttributes,
                MessageBody = MessageBody,
                QueueUrl = QueueUrl,

            };
            if (MessageDeduplicationId.Length > 0)
            {
                // For Fifo queues
                sendMessageRequest.MessageDeduplicationId = MessageDeduplicationId ?? getCurrentTimeInMillis().ToString();
                sendMessageRequest.MessageGroupId = MessageGroupId ?? getCurrentTimeInMillis().ToString();
            }
            else
            {
                // For standard queues
                sendMessageRequest.DelaySeconds = 10;
            }
            var response = await client.SendMessageAsync(sendMessageRequest);
            return response.MessageId;
        }
        public string Send_Message_Ext(AWS_Authenticationinfo authInfo, string QueueUrl, string MessageBody, string MessageDeduplicationId, string MessageGroupId, List<SQS_MessageAttribute> MessageAttributes)
        {
            AmazonSQSClient client = getClient(authInfo);
            var response = Send_Message(client, QueueUrl, MessageBody, MessageDeduplicationId, MessageGroupId, MessageAttributes);
            return response.Result;
        }
        private async Task<List<SQS_Message>> Receive_Message(AmazonSQSClient client, string QueueUrl, int MaxNumberOfMessages, List<SQS_MessageAttribute> MessageAttributeNames, int WaitTimeSeconds)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = QueueUrl,
                MaxNumberOfMessages = MaxNumberOfMessages,
            };

            // Map provided SQS_MessageAttribute names into the AWS SDK request (if any)
            if (MessageAttributeNames != null && MessageAttributeNames.Count > 0)
            {
                receiveMessageRequest.MessageAttributeNames = MessageAttributeNames.Select(ma => ma.Name).ToList();
            }

            // If WaitTimeSeconds provided, clamp to SQS max (20) and set
            if (WaitTimeSeconds > 0)
            {
                receiveMessageRequest.WaitTimeSeconds = Math.Min(WaitTimeSeconds, 20);
            }

            var receiveMessageResponse = await client.ReceiveMessageAsync(receiveMessageRequest);
            if (receiveMessageResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                var result = new List<SQS_Message>();
                foreach (var message in receiveMessageResponse.Messages)
                {
                    var msg = new SQS_Message()
                    {
                        MessageId = message.MessageId,
                        MessageBody = message.Body,
                        ReceiptHandle = message.ReceiptHandle,
                        MessageAttributes = new List<SQS_MessageAttribute>(),
                    };

                    foreach (var key in message.MessageAttributes.Keys)
                    {
                        MessageAttributeValue ma = new MessageAttributeValue();
                        var t = message.MessageAttributes[key]?.ToString();
                        if (!string.IsNullOrEmpty(t))
                        {
                            var sqs_ma = new SQS_MessageAttribute()
                            {
                                Name = key,
                                Value = message.MessageAttributes[key].StringValue ?? "",
                                DataType = message.MessageAttributes[key].DataType ?? "String",
                            };
                            msg.MessageAttributes.Add(sqs_ma);
                        }
                    }
                    result.Add(msg);
                }
                return result;
            }
            else
            {
                return new List<SQS_Message>();
            }
        }
        public List<SQS_Message> Receive_Message_Ext(AWS_Authenticationinfo authInfo, string QueueUrl, int MaxNumberOfMessages, List<SQS_MessageAttribute> MessageAttributeNames, int WaitTimeSeconds)
        {
            AmazonSQSClient client = getClient(authInfo);
            var response = Receive_Message(client, QueueUrl, MaxNumberOfMessages, MessageAttributeNames, WaitTimeSeconds);
            return response.Result;
        }

        private async Task<bool> Purge_Queue(AmazonSQSClient client, string QueueUrl)
        {
            var response = await client.PurgeQueueAsync(QueueUrl);
            return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }

        public bool Purge_Queue_Ext(AWS_Authenticationinfo authInfo, string QueueUrl)
        {
            AmazonSQSClient client = getClient(authInfo);
            return Purge_Queue(client, QueueUrl).Result;
        }
        private string ReadResource(string name)
        {
            var assembly = Assembly.GetExecutingAssembly();
            string resourcePath = name;
            if (assembly.GetManifestResourceStream(resourcePath) != null)
            {
                using (Stream stream = assembly.GetManifestResourceStream(resourcePath)!)
                {
                    using (StreamReader reader = new StreamReader(stream))
                    {
                        return reader.ReadToEnd();
                    }
                }
            }
            return string.Empty;
        }


        public string GetBuildInfo_Ext()
        {
            return ReadResource("psn.PH.buildinfo.txt");
        }
    }
}
