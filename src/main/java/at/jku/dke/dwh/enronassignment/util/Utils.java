package at.jku.dke.dwh.enronassignment.util;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    public static void printArrayList(List<String> arrayList) {
        if (arrayList.isEmpty()) {
            System.out.println("ArrayList is empty!");
            return;
        }
        for (String entry : arrayList) {
            System.out.println(entry);
        }
    }

    public static ArrayList<String> removeTabs(List<String> list) {
        ArrayList<String> result = new ArrayList<>();
        for (String str : list) {
            if (str.contains("To:") || str.contains("Cc:") || str.contains("Bcc:") || str.contains("X-To:") || str.contains("X-cc:") || str.contains("X-bcc:")) {
                //replace tabs and double-spaces
                result.add(str.replace("\t", "").replace("  ", " "));
            } else {
                result.add(str);
            }
        }
        return result;
    }
}
