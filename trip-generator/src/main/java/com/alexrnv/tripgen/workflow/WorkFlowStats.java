package com.alexrnv.tripgen.workflow;

import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author ARyazanov
 *         3/25/2016.
 */
public class WorkFlowStats {

    //0-created, 1-updated, 2-completed
    private final AtomicIntegerArray workFlowCounters = new AtomicIntegerArray(3);
    private final List<BoundedStatContainer> workFlowTimers = new ArrayList<>(4);

    public WorkFlowStats() {
        workFlowTimers.add(0, new BoundedStatContainer("Find", 10000));
        workFlowTimers.add(1, new BoundedStatContainer("Create", 10000));
        workFlowTimers.add(2, new BoundedStatContainer("Update", 10000));
        workFlowTimers.add(3, new BoundedStatContainer("Complete", 10000));
        workFlowTimers.add(4, new BoundedStatContainer("All", 10000));
    }

    public void addFindTime(long t) {
        workFlowTimers.get(0).add(t);
    }

    public void addCreateTime(long t) {
        workFlowTimers.get(1).add(t);
    }

    public void addUpdateTime(long t) {
        workFlowTimers.get(2).add(t);
    }

    public void addCompleteTime(long t) {
        workFlowTimers.get(3).add(t);
    }

    public void addAllTime(long t) {
        workFlowTimers.get(4).add(t);
    }

    public void incCreated() {
        workFlowCounters.incrementAndGet(0);
    }

    public void incUpdated() {
        workFlowCounters.incrementAndGet(1);
    }

    public void incCompleted() {
        workFlowCounters.incrementAndGet(2);
    }

    public int getAndResetCreated() {
        return workFlowCounters.getAndSet(0, 0);
    }

    public int getAndResetUpdated() {
        return workFlowCounters.getAndSet(1, 0);
    }

    public int getAndResetCompleted() {
        return workFlowCounters.getAndSet(2, 0);
    }

    public double getMeanTimeFind() {
        return workFlowTimers.get(0).getMean();
    }

    public double getMeanTimeCreate() {
        return workFlowTimers.get(1).getMean();
    }

    public double getMeanTimeUpdate() {
        return workFlowTimers.get(2).getMean();
    }

    public double getMeanTimeComplete() {
        return workFlowTimers.get(3).getMean();
    }

    public double getMeanTimeAll() {
        return workFlowTimers.get(4).getMean();
    }

    //this stat holder is not fair as it uses only first <capacity> points, throwing out the rest
    //though it is enough to understand the trend
    private static class BoundedStatContainer {
        private final AtomicInteger pointer = new AtomicInteger();
        private volatile int arrayPointer = 0;
        private final double[][] array;
        public final String name;

        public BoundedStatContainer(String name, int capacity) {
            this.name = name;
            array = new double[2][];
            array[0] = new double[capacity];
            array[1] = new double[capacity];
        }

        public void add(long v) {
            int ptr = pointer.getAndIncrement();
            if(ptr < array[arrayPointer].length) {
                array[arrayPointer][ptr] = v;
            }
        }

        public double getMean() {
            int prev = arrayPointer;
            arrayPointer = (arrayPointer+1)%2;
            pointer.getAndSet(0);
            double d = StatUtils.mean(array[prev]);
            Arrays.fill(array[prev], 0);
            return d;
        }
    }
}
