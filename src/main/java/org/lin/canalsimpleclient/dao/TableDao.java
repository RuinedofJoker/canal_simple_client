package org.lin.canalsimpleclient.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.lin.canalsimpleclient.dto.Table;

@Mapper
public interface TableDao extends BaseMapper<Table> {

    boolean insertTestCol(Table table);
}
