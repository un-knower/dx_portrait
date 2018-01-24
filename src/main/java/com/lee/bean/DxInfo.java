package com.lee.bean;

import java.util.List;

/**
 * Created by root on 2018/1/19.
 */
public class DxInfo {

    /**
     * _id : cee46b6eba8581da3d234bc6a0cf30d0
     * apps : [{"atime":"2017-10-20","v":1,"k":"AP2644"},{"atime":"2017-10-20","v":1,"k":"AP2753"},{"atime":"2017-10-20","v":1,"k":"AP3635"},{"atime":"2017-10-20","v":2,"k":"AP2375"},{"atime":"2017-10-24","v":1,"k":"AP3018"},{"atime":"2017-12-22","v":13,"f":13,"k":"AP2984"},{"atime":"2017-12-18","v":6,"f":6,"k":"AP2842"},{"atime":"2018-01-13","v":4,"f":4,"t_t":"1_000000000000100000000000","k":"AP2649"}]
     * others : {"g":[{"k":"F8ED2mAe+8nePzt2299V1A=="}],"x":[{"k":"Vb8H0lb278oTC8aRfvxL9A=="}],"sl":[{"k":"1ymjJGZh6RGZbW/WKTUqI7ffOKL/kt/vtkwMZebQeQOim8+3kQPfu08krUhK1yCs"}]}
     */

    public String _id;
    public Others others;
    public List<Apps> apps;

    public static class Others {
        public List<G> g;
        public List<G> x;

        public static class G {
            /**
             * k : F8ED2mAe+8nePzt2299V1A==
             */

            public String k;
        }
    }

    public static class Apps {
        /**
         * atime : 2017-10-20
         * v : 1
         * k : AP2644
         * f : 13
         * t_t : 1_000000000000100000000000
         */

        public String atime;
        public int v;
        public String k;
        public int f;
        public String t_t;
    }
}
