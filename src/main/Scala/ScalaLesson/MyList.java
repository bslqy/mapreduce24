package ScalaLesson;

import java.util.ArrayList;

public class MyList {

    private ArrayList<Integer> list;
    private ArrayList<Integer> resultList;

    public MyList(ArrayList<Integer> list){
        this.list = list;
    }

    public ArrayList<Integer> map(Operate op){
        resultList =  new ArrayList<Integer>();
        for (Integer i: list){
            resultList.add((Integer)op.operate(i));
        }
        return resultList;

    }

    public static void main(String[] args) {

        ArrayList list = new ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        MyList myList = new MyList(list);
        ArrayList rList = myList.map(new Operate() {
            @Override
            public Object operate(Integer i) {
                return i * 10;
            }
        });

        for (Object i : rList){
            System.out.println(i);
        }
    }

}
