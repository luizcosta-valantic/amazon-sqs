using Xunit;
using Xunit.Abstractions;

using psn.PH.Structures;
namespace psn.PH;

public class AWS_SQS_ExtTests
{
    private string aws_sqs_access_id = Environment.GetEnvironmentVariable("AWS_SQS_ACCESS_ID") ?? "Sample_value";
    private string aws_sqs_secret_access_key = Environment.GetEnvironmentVariable("AWS_SQS_SECRET_ACCESS_KEY") ?? "Sample_value";
    private string aws_sqs_region = Environment.GetEnvironmentVariable("AWS_SQS_REGION") ?? "Sample_value";
    private readonly ITestOutputHelper output;
    static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    AWS_Authenticationinfo authInfo;

    public AWS_SQS_ExtTests(ITestOutputHelper output)
    {
        this.output = output;
        this.authInfo = getAuthInfo();
    }
    private AWS_Authenticationinfo getAuthInfo()
    {
        AWS_Authenticationinfo authInfo = new AWS_Authenticationinfo()
        {
            Region = aws_sqs_region,
            AccessKeyId = aws_sqs_access_id.Trim(),
            SecretAccessKey = aws_sqs_secret_access_key.Trim(),
        };
        output.WriteLine("Region = " + authInfo.Region);
        return authInfo;
    }

    private long getCurrentTimeInMillis()
    {
        long ms = (long)(DateTime.UtcNow - epoch).TotalMilliseconds;
        long result = ms / 1000;
        return result;
    }
    [Fact]
    public void Create_Delete_Queue_Ext_test1()
    {
        AWS_SQS_Ext client = new AWS_SQS_Ext();
        string queueName = "Create_Queue_Ext_test1-" + getCurrentTimeInMillis();
        string queueURL = client.Create_Queue_Ext(authInfo, queueName, true, true);
        output.WriteLine("queueURL = " + queueURL);
        Assert.StartsWith("https://sqs", queueURL);
        Assert.EndsWith(queueName + ".fifo", queueURL);
        bool isSuccessful = client.Delete_Queue_Ext(authInfo, queueURL);
        Assert.True(isSuccessful);
    }

    [Fact]
    public void Send_Message_Ext_text1()
    {
        AWS_SQS_Ext client = new AWS_SQS_Ext();
        string queueURL = client.Create_Queue_Ext(authInfo, "Send_Message_Ext_text1-" + getCurrentTimeInMillis(), true, true);
        List<SQS_MessageAttribute> msgAttr = new List<SQS_MessageAttribute>();
        string messageId = client.Send_Message_Ext(authInfo, queueURL, "Hello World!", getCurrentTimeInMillis().ToString(), getCurrentTimeInMillis().ToString(), msgAttr);

        List<SQS_Message> msgs = client.Receive_Message_Ext(authInfo, queueURL, 10, new List<SQS_MessageAttribute>(), 0);
        Assert.True(msgs.Count > 0);

        foreach (var msg in msgs)
        {
            output.WriteLine("Message.ID = " + msg.MessageId);
            Assert.True(msg.MessageId.Length > 0);
            output.WriteLine("Message.Body = " + msg.MessageBody);
            output.WriteLine("Message.ReceiptHandle = " + msg.ReceiptHandle);
            output.WriteLine("=========================================");
        }
        bool isSuccessful = client.Delete_Queue_Ext(authInfo, queueURL);
        Assert.True(isSuccessful);
    }

    [Fact]
    public void Send_Message_Ext_text2()
    {
        AWS_SQS_Ext client = new AWS_SQS_Ext();
        string queueURL = client.Create_Queue_Ext(authInfo, "Send_Message_Ext_text2-" + getCurrentTimeInMillis(), false, false);
        List<SQS_MessageAttribute> msgAttr = new List<SQS_MessageAttribute>();
        string messageId = client.Send_Message_Ext(authInfo, queueURL, "Hello World!", string.Empty, string.Empty, msgAttr);

        List<SQS_Message> msgs = client.Receive_Message_Ext(authInfo, queueURL, 10, new List<SQS_MessageAttribute>(), 0);
        Assert.True(messageId.Length > 0);
        // note that for standard queue, it is fire and forget. we should not expect the message to appear immediately in the queue but we should have a message ID as a response
        output.WriteLine("Send_Message_Ext_text2 MessageID =" + messageId);
        foreach (var msg in msgs)
        {
            output.WriteLine("Message.ID = " + msg.MessageId);
            Assert.True(msg.MessageId.Length > 0);
            output.WriteLine("Message.Body = " + msg.MessageBody);
            output.WriteLine("Message.ReceiptHandle = " + msg.ReceiptHandle);
            output.WriteLine("=========================================");
        }
        bool isSuccessful = client.Delete_Queue_Ext(authInfo, queueURL);
        Assert.True(isSuccessful);
    }

    [Fact]
    public void GetBuildInfo_Ext_test1()
    {
        var se = new AWS_SQS_Ext();
        string buildInfo = se.GetBuildInfo_Ext();
        output.WriteLine(buildInfo);
        Assert.True(buildInfo.Length > 0);
    }
}