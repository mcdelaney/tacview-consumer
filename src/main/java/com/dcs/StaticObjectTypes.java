package com.dcs;

import java.util.Arrays;
import java.util.List;

class StaticObjectElement {
    List<String> values;

    public StaticObjectElement() {
    }

    public boolean checkValueEqualsAny(String type_value) {
        if (type_value == null) {
            return false;
        }
        return this.values.contains(type_value);
    }

    public List<String> Get() {
        return this.values;
    }
}

class ImpactTypes extends StaticObjectElement {

    public ImpactTypes() {
        super();
        this.values = Arrays.asList("Weapon+Missile", "Weapon+Bomb", "Projectile+Shell");
    }
}

class ParentTypes extends StaticObjectElement {
    public ParentTypes() {
        super();
        this.values = Arrays.asList("Weapon+Missile", "Projectile+Shell", "Misc+Decoy+Flare",
                "Misc+Decoy+Chaff", "Misc+Container", "Misc+Shrapnel", "Ground+Light+Human+Air+Parachutist");
    }
}
