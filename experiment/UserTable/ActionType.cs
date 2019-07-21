namespace Howlett.Kafka.Extensions.Experiment
{
    public enum ActionType
    {
        /// <summary>
        ///     set the column value / data.
        /// </summary>
        Set,

        /// <summary>
        ///     delete the column value / data.
        /// </summary>
        Delete,

        /// <summary>
        ///     abort the operation (no change)
        /// </summary>
        Abort
    }
}
