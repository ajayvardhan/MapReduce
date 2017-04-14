package org.apache.maven.graph;

import org.apache.hadoop.io.*;
import java.io.*;

/*
Custom writable comparable object to be used as a node object. A node's adjacency list, pagerank and a boolean to say
if this is a node or not is stored in each node object. When a node is emitted from the adjacency list, the boolean
is set as false as this is just used to calculate pagerank.
 */

public class WritableComparableObject implements WritableComparable<WritableComparableObject> {
    private Text pageRank, adjacencyList, isNode;
    public WritableComparableObject(){
        this.pageRank = new Text();
        this.adjacencyList = new Text();
        this.isNode = new Text("false");
    }
    public WritableComparableObject(Text pageRank, Text adjacencyList){
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
        this.isNode = new Text("true");
    }
    public WritableComparableObject(Text pageRank){
        this.pageRank = pageRank;
        this.adjacencyList = new Text();
        this.isNode = new Text("false");
    }
    public void write(DataOutput out) throws IOException {
        this.pageRank.write(out);
        this.adjacencyList.write(out);
        this.isNode.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.pageRank.readFields(in);
        this.adjacencyList.readFields(in);
        this.isNode.readFields(in);
    }

    public int compareTo(WritableComparableObject o) {
        int temp = this.pageRank.compareTo(o.pageRank);
        if (temp == 0){
            int temp1 = this.adjacencyList.compareTo(o.adjacencyList);
            if (temp1 == 0) {
                return this.isNode.compareTo(o.isNode);
            }
            else{
                return temp1;
            }
        }
        return temp;
    }

    public int hashCode() {
        return (((Math.abs(this.pageRank.hashCode() + this.isNode.hashCode() + this.adjacencyList.hashCode()))*29)+967);
    }

    public Text getpageRank(){
        return this.pageRank;
    }
    public Text getadjacencyList(){
        return this.adjacencyList;
    }
    public Text getisNode(){ return this.isNode;  }
    public void setpageRank(Text pageRank){
        this.pageRank = pageRank;
    }
    public void setadjacencyList(Text adjacencyList){
        this.adjacencyList = adjacencyList;
    }
    public void setisNode(Text isNode){ this.isNode = isNode;  }
}