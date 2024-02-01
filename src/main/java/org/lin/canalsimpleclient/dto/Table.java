package org.lin.canalsimpleclient.dto;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

@TableName("table1")
@Data
@Accessors(chain = true)
public class Table {

    @TableId
    private Integer id;

    private String testCol;
}
