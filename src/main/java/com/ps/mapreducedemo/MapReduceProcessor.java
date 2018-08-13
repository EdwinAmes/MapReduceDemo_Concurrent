package com.ps.mapreducedemo;

import com.ps.mapreducedemo.util.IoUtils;
import java.util.concurrent.Callable;

/**
 * Created by Edwin on 4/21/2016.
 *
 */
public abstract class MapReduceProcessor implements Runnable, Callable<Boolean> {
    protected IoUtils ioUtils;
    public MapReduceProcessor(IoUtils ioUtils) {
        this.ioUtils = ioUtils;
    }

}
