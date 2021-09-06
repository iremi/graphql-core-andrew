package com.linkedin.datahub.graphql.types.dashboard2.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard2.Dashboard2;
import com.linkedin.dashboard2.EditableDashboard2Properties;
import com.linkedin.datahub.graphql.generated.Dashboard2UpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class Dashboard2UpdateInputMapper implements InputModelMapper<Dashboard2UpdateInput, Dashboard2, Urn> {
    public static final Dashboard2UpdateInputMapper INSTANCE = new Dashboard2UpdateInputMapper();

    public static Dashboard2 map(@Nonnull final Dashboard2UpdateInput dashboard2UpdateInput,
                                 @Nonnull final Urn actor) {
        return INSTANCE.apply(dashboard2UpdateInput, actor);
    }

    @Override
    public Dashboard2 apply(@Nonnull final Dashboard2UpdateInput dashboard2UpdateInput,
                            @Nonnull final Urn actor) {
        final Dashboard2 result = new Dashboard2();

        if (dashboard2UpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(dashboard2UpdateInput.getOwnership(), actor));
        }

        if (dashboard2UpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            dashboard2UpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }

        if (dashboard2UpdateInput.getEditableProperties() != null) {
            final EditableDashboard2Properties editableDashboard2Properties = new EditableDashboard2Properties();
            editableDashboard2Properties.setDescription(dashboard2UpdateInput.getEditableProperties().getDescription());
            result.setEditableProperties(editableDashboard2Properties);
        }
        return result;
    }

}
