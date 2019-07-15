namespace Howlett.Kafka.Extensions.Experiment
{
    public enum CommandType
    {
        /// <summary>
        ///     The initial command.
        /// </summary>
        Change,

        /// <summary>
        ///     Send to other columns to inform of the new value.
        /// </summary>
        Lock,

        /// <summary>
        ///     Columns send this back confirming or not that the value is good to change.
        /// </summary>
        Verify,

        /// <summary>
        ///     Sent to columns with a locked value to unlock it.
        /// </summary>
        Unlock,

        /// <summary>
        ///     Sent from the change log consume loop to acknowledge the change.
        /// </summary>
        Ack
    }
}
