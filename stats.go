package queue

import (
	"bytes"
	"fmt"
)

type ConnectionStat struct {
	UnackedCount int
	Consumers    []string
}

func (stat ConnectionStat) String() string {
	return fmt.Sprintf("[unacked:%d consumers:%d]",
		stat.UnackedCount,
		len(stat.Consumers),
	)
}

type ConnectionStats map[string]ConnectionStat

type QueueStat struct {
	ReadyCount      int
	ConnectionStats ConnectionStats
}

func NewQueueStat(readyCount int) QueueStat {
	return QueueStat{
		ReadyCount:      readyCount,
		ConnectionStats: ConnectionStats{},
	}
}

func (stat QueueStat) String() string {
	return fmt.Sprintf("[ready:%d conn:%s",
		stat.ReadyCount,
		stat.ConnectionStats,
	)
}

func (stat QueueStat) UnackedCount() int {
	unacked := 0
	for _, connectionStat := range stat.ConnectionStats {
		unacked += connectionStat.UnackedCount
	}
	return unacked
}

func (stat QueueStat) ConsumerCount() int {
	consumer := 0
	for _, connectionStat := range stat.ConnectionStats {
		consumer += len(connectionStat.Consumers)
	}
	return consumer
}

type QueueStats map[string]QueueStat

func CollectStats(mainConnection *Connection) QueueStats {
	queueStats := QueueStats{}
	for _, queueName := range mainConnection.GetOpenQueues() {
		queue := mainConnection.openQueue(queueName)
		queueStats[queueName] = NewQueueStat(queue.ReadyCount())
	}

	connectionNames := mainConnection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := mainConnection.hijackConnection(connectionName)
		queueNames := connection.GetConsumingQueues()

		for _, queueName := range queueNames {
			queue := connection.openQueue(queueName)
			consumers := queue.GetConsumers()

			queueStats[queueName].ConnectionStats[connectionName] = ConnectionStat{
				UnackedCount: queue.UnackedCount(),
				Consumers:    consumers,
			}
		}
	}

	return queueStats
}

func (stats QueueStats) String() string {
	var buffer bytes.Buffer

	for queueName, queueStat := range stats {
		buffer.WriteString(fmt.Sprintf("    queue:%s ready:%d unacked:%d consumers:%d\n",
			queueName, queueStat.ReadyCount, queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			buffer.WriteString(fmt.Sprintf("        connection:%s unacked:%d consumers:%d\n",
				connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
		}
	}

	return buffer.String()
}

func (stats QueueStats) GetHtml() string {
	var buffer bytes.Buffer
	buffer.WriteString(`<html><body><table style="font-family:monospace">`)
	buffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%s</td><td></td><td style="color:lightgrey">%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td></tr>`,
		"queue", "ready", "connection", "unacked", "consumers",
	))

	for queueName, queueStat := range stats {
		buffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%d</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
			queueName, queueStat.ReadyCount, "", queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			buffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
				"", "", connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
		}
	}

	buffer.WriteString(`</table></body></html>`)
	return buffer.String()
}
