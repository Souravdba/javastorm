package com.swayam.storm.jms;

import java.util.Map;

import org.apache.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class GenericBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(GenericBolt.class);
    private OutputCollector collector;
    private boolean autoAck = false;
    private boolean autoAnchor = false;
    private Fields declaredFields;
    private String name;

    /**
     * Constructs a new <code>GenericBolt</code> instance.
     *
     * @param name           The name of the bolt (used in DEBUG logging)
     * @param autoAck        Whether or not this bolt should automatically acknowledge received tuples.
     * @param autoAnchor     Whether or not this bolt should automatically anchor to received tuples.
     * @param declaredFields The fields this bolt declares as output.
     */
    public GenericBolt(String name, boolean autoAck, boolean autoAnchor, Fields declaredFields) {
        this.name = name;
        this.autoAck = autoAck;
        this.autoAnchor = autoAnchor;
        this.declaredFields = declaredFields;
    }

    public GenericBolt(String name, boolean autoAck, boolean autoAnchor) {
        this(name, autoAck, autoAnchor, null);
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

    }

    public void execute(Tuple input) {
        LOG.debug("[" + this.name + "] Received message: " + input);


        // only emit if we have declared fields.
        if (this.declaredFields != null) {
            LOG.debug("[" + this.name + "] emitting: " + input);
            if (this.autoAnchor) {
                this.collector.emit(input, input.getValues());
            } else {
                this.collector.emit(input.getValues());
            }
        }

        if (this.autoAck) {
            LOG.debug("[" + this.name + "] ACKing tuple: " + input);
            this.collector.ack(input);
        }

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.declaredFields != null) {
            declarer.declare(this.declaredFields);
        }
    }

    public boolean isAutoAck() {
        return this.autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }


}
