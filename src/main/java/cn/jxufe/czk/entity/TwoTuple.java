package cn.jxufe.czk.entity;


public class TwoTuple {

    private String twoTuple;
    private Long num;
    private Double p;
    public Double getP() {
        return p;
    }
    public void setP(Double p) {
        this.p = p;
    }
    public String getTwoTuple() {
        return twoTuple;
    }
    public void setTwoTuple(String twoTuple) {
        this.twoTuple = twoTuple;
    }
    public Long getNum() {
        return num;
    }
    public void setNum(Long num) {
        this.num = num;
    }
    @Override
    public String toString() {
        return "TwoTuple [twoTuple=" + twoTuple + ", num=" + num + ", p=" + p + "]";
    }

}
