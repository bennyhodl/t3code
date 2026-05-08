import { MonitorCheckIcon, ServerIcon, SettingsIcon } from "lucide-react";
import { memo, useCallback } from "react";
import { Link, useNavigate } from "@tanstack/react-router";

import { LinearSidebarStatus } from "../linear/LinearSidebarStatus";
import { ServicesSidebarStatus } from "../services/ServicesSidebarStatus";
import { SetupSidebarStatus } from "../setup/SetupSidebarStatus";

import { useEnvironmentIdentificationMode } from "../../hooks/useSettings";
import { cn } from "../../lib/utils";
import {
  resolveEnvironmentIdentificationPillLabel,
  resolveSidebarStageBackdropVariant,
  SidebarStageBackdrop,
  useEnvironmentStageLabel,
} from "../SidebarStageBackdrop";
import { Badge } from "../ui/badge";
import {
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarTrigger,
  useSidebar,
} from "../ui/sidebar";
import { SidebarProviderUpdatePill } from "./SidebarProviderUpdatePill";
import { SidebarUpdatePill } from "./SidebarUpdatePill";

export const SidebarChromeHeader = memo(function SidebarChromeHeader({
  isElectron,
}: {
  isElectron: boolean;
}) {
  const stageLabel = useEnvironmentStageLabel();
  const environmentIdentificationMode = useEnvironmentIdentificationMode();
  const backdropVariant = resolveSidebarStageBackdropVariant(
    stageLabel,
    environmentIdentificationMode === "artwork",
  );
  const pillLabel =
    environmentIdentificationMode === "pill"
      ? resolveEnvironmentIdentificationPillLabel(stageLabel)
      : null;

  return (
    <SidebarHeader
      className={cn(
        "@container/sidebar-header relative h-[var(--workspace-topbar-height)] shrink-0 flex-row items-center px-3 py-0 md:px-0",
        isElectron && "drag-region",
      )}
    >
      {backdropVariant ? <SidebarStageBackdrop variant={backdropVariant} /> : null}
      <SidebarTrigger
        className={cn(
          "relative z-10 md:hidden",
          backdropVariant &&
            "[:hover,[data-pressed]]:bg-white/15 focus-visible:ring-white/90 focus-visible:ring-offset-blue-700 [&_svg]:stroke-white/90! [&_svg]:opacity-100! [&_svg]:hover:stroke-white!",
        )}
      />
      <SidebarBrand onBackdrop={backdropVariant !== null} />
      {pillLabel ? (
        <Badge
          className="relative z-10 ml-1 rounded-full px-1.5 text-muted-foreground"
          data-environment-identification="pill"
          size="sm"
          variant="secondary"
        >
          {pillLabel}
        </Badge>
      ) : null}
    </SidebarHeader>
  );
});

function SidebarBrand({ onBackdrop }: { onBackdrop: boolean }) {
  return (
    <Link
      aria-label="Go to threads"
      className={cn(
        "sidebar-brand relative z-10 ml-[var(--workspace-titlebar-content-left)] h-7 w-fit min-w-0 shrink-0 items-center gap-1 overflow-hidden rounded-md outline-hidden ring-ring focus-visible:ring-2",
        onBackdrop ? "text-white" : "text-foreground",
      )}
      to="/"
    >
      <LygosLogo />
      <span
        className={cn(
          "truncate text-sm font-medium tracking-tight",
          onBackdrop ? "text-white/70" : "text-muted-foreground",
        )}
      >
        Dev
      </span>
    </Link>
  );
}

function LygosLogo() {
  return (
    <svg
      aria-label="Lygos"
      width="20"
      height="20"
      viewBox="0 0 58 58"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className="shrink-0"
    >
      <path
        d="M0.108 33.745a1.47 1.47 0 0 1-.108-.538V1.466C0 .656.325 0 1.468 0h11.522c.389 0 .762.156 1.037.43l10.131 10.118a1.464 1.464 0 0 1 0 2.072L2.506 34.243a1.468 1.468 0 0 1-2.398-.498Z"
        fill="#00ACC1"
      />
      <path
        d="M57.753 1.466v11.506a1.46 1.46 0 0 1-.108.557 1.47 1.47 0 0 1-.323.48L47.191 24.125a1.467 1.467 0 0 1-2.075 0L23.463 2.503a1.468 1.468 0 0 1 .48-2.395A1.47 1.47 0 0 1 24.501 0h31.784c.81 0 1.468.656 1.468 1.466Z"
        fill="#00ACC1"
      />
      <path
        d="M57.646 23.93c.07.17.107.353.107.538v31.74a1.468 1.468 0 0 1-1.468 1.467H44.764a1.467 1.467 0 0 1-1.038-.43L33.595 47.127a1.464 1.464 0 0 1 0-2.072l21.652-21.623a1.468 1.468 0 0 1 2.399.498Z"
        fill="#00ACC1"
      />
      <path
        d="M16.318 27.801 27.839 16.296a1.467 1.467 0 0 1 2.075 0L41.435 27.8a1.464 1.464 0 0 1 0 2.073L29.914 41.38a1.467 1.467 0 0 1-2.075 0L16.318 29.874a1.464 1.464 0 0 1 0-2.073Z"
        fill="#00ACC1"
      />
      <path
        d="M34.613 55.671a1.467 1.467 0 0 1-1.36 2.004H1.468A1.468 1.468 0 0 1 0 56.209V44.703c.006-.19.042-.378.108-.557a1.47 1.47 0 0 1 .323-.48L10.562 33.55a1.467 1.467 0 0 1 2.075 0l21.652 21.623c.146.138.257.31.324.498Z"
        fill="#00ACC1"
      />
    </svg>
  );
}

export const SidebarChromeFooter = memo(function SidebarChromeFooter() {
  const navigate = useNavigate();
  const { isMobile, setOpenMobile } = useSidebar();
  const navigateTo = useCallback(
    (to: string) => {
      if (isMobile) {
        setOpenMobile(false);
      }
      void navigate({ to });
    },
    [isMobile, navigate, setOpenMobile],
  );
  const handleServicesClick = useCallback(() => navigateTo("/services"), [navigateTo]);
  const handleLinearClick = useCallback(() => navigateTo("/linear"), [navigateTo]);
  const handleSetupClick = useCallback(() => navigateTo("/setup"), [navigateTo]);
  const handleSettingsClick = useCallback(() => navigateTo("/settings"), [navigateTo]);

  return (
    <SidebarFooter className="p-2">
      <SidebarProviderUpdatePill />
      <SidebarUpdatePill />
      <SidebarMenu>
        {/* Lygos fork navigation entries. */}
        <SidebarMenuItem>
          <SidebarMenuButton onClick={handleServicesClick}>
            <ServerIcon />
            <span>Services</span>
            <ServicesSidebarStatus />
          </SidebarMenuButton>
        </SidebarMenuItem>
        <SidebarMenuItem>
          <SidebarMenuButton onClick={handleLinearClick}>
            <LinearIcon />
            <span>Linear</span>
            <LinearSidebarStatus />
          </SidebarMenuButton>
        </SidebarMenuItem>
        <SidebarMenuItem>
          <SidebarMenuButton onClick={handleSetupClick}>
            <MonitorCheckIcon />
            <span>Setup</span>
            <SetupSidebarStatus />
          </SidebarMenuButton>
        </SidebarMenuItem>
        <SidebarMenuItem>
          <SidebarMenuButton onClick={handleSettingsClick}>
            <SettingsIcon />
            <span>Settings</span>
          </SidebarMenuButton>
        </SidebarMenuItem>
      </SidebarMenu>
    </SidebarFooter>
  );
});

function LinearIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 100 100"
      className="size-3.5"
      aria-hidden="true"
    >
      <path
        fill="currentColor"
        d="M1.225 61.523c-.222-.949.908-1.546 1.597-.857L39.334 97.178c.689.689.092 1.819-.857 1.596C20.052 94.452 5.548 79.949 1.225 61.523ZM.002 46.889a1.075 1.075 0 0 1 .29.761L52.35 99.709c.2.2.477.307.76.289 2.37-.147 4.694-.46 6.963-.926.764-.157 1.03-1.096.478-1.648L2.576 39.449c-.552-.552-1.491-.287-1.648.478A50.08 50.08 0 0 0 .002 46.89ZM4.211 29.705a1.074 1.074 0 0 1 .208 1.1l64.776 64.776c.29.29.726.374 1.1.208a49.648 49.648 0 0 0 5.186-2.684c.552-.328.637-1.087.183-1.541L8.436 24.337c-.454-.454-1.213-.369-1.541.183a49.662 49.662 0 0 0-2.684 5.185ZM12.659 18.074c-.37-.37-.393-.964-.044-1.354C21.78 6.46 35.111 0 49.952 0 77.593 0 100 22.407 100 50.048c0 14.84-6.46 28.172-16.72 37.338-.39.348-.984.326-1.354-.045L12.659 18.074Z"
      />
    </svg>
  );
}
