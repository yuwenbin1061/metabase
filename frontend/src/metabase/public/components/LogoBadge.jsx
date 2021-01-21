/* @flow */

import React from "react";
import LogoIcon from "metabase/components/LogoIcon";

import { t, jt } from "ttag";

type Props = {
  dark: boolean,
};

const LogoBadge = ({ dark }: Props) => (
  <a className="h4 flex text-bold align-center no-decoration">
    <span className="text-small">
      <span className="ml1 md-ml2 text-medium"></span>
    </span>
  </a>
);

export default LogoBadge;
