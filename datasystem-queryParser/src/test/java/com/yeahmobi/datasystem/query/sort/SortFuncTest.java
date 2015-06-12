package com.yeahmobi.datasystem.query.sort;
/**
 * Created by yangxu on 5/12/14.
 */

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.query.dimension.DimensionType;
import io.druid.query.groupby.orderby.TopNSorter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SortFuncTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: run()
     *
     */
    @Test
    public void testNature() {
        String[] nums = new String[] {"1", "2", "11", "14044", "13207", "100005"};
        List<String> res = Ordering.natural().nullsFirst().sortedCopy(Arrays.asList(nums));

        System.out.println(Long.valueOf("1000"));
        System.out.println(Joiner.on(',').join(res));
    }

    class TestRow {
        long val;
        String offer;
        TestRow(long val, String offer) {
            this.val = val;
            this.offer = offer;
        }

        public String getOffer() {
            return offer;
        }

        @Override
        public String toString() {
            return val + ":" + offer;
        }

        public long getVal() {
            return val;
        }

        public void setVal(long val) {
            this.val = val;
        }

        public void setOffer(String offer) {
            this.offer = offer;
        }
    }

    @Test
    public void testFuncCompose() {
        List<TestRow> rows = new ArrayList<>();
        rows.add(new TestRow(12345, "14044"));
        rows.add(new TestRow(12345, "100005"));
        rows.add(new TestRow(12345, "200005"));
        rows.add(new TestRow(12345, "19670"));
        rows.add(new TestRow(12346, "14044"));
        rows.add(new TestRow(12347, "100005"));
        rows.add(new TestRow(12348, "200005"));
        rows.add(new TestRow(12346, "19670"));

        Ordering first = new Ordering<TestRow>()
        {
            @Override
            public int compare(TestRow left, TestRow right)
            {
                return Longs.compare(left.getVal(), right.getVal());
            }
        };
        Ordering ordering = first.compound(DimensionType.INTEGER.getOrdering().nullsFirst().onResultOf(
                new Function<TestRow, Comparable>() {
                    @Nullable
                    @Override
                    public Comparable apply(@Nullable TestRow input) {
                        Comparable cmp = DimensionType.INTEGER.cast(input.getOffer());
                        System.out.println(cmp);
                        return cmp;
                    }
                }
        ));

        TopNSorter sorter = new TopNSorter(ordering.reverse());

        System.out.println(Joiner.on(",").join(sorter.toTopN(rows, 5)));

    }

}
