export const DisableInputExtension = {
  name: "DisableInput",
  type: "effect",
  match: ({ trace }) =>
    trace.type === "ext_disableInput" ||
    trace.payload.name === "ext_disableInput",
  effect: ({ trace }) => {
    const { isDisabled } = trace.payload;

    const disableInputs = (isDisabled) => {
      const chatDiv = document.getElementById("voiceflow-chat");
      const shadowRoot = chatDiv?.shadowRoot;
      
      shadowRoot?.querySelectorAll(".vfrc-chat-input")?.forEach((element) => {
        element.disabled = isDisabled;
        element.style.pointerEvents = isDisabled ? "none" : "auto";
        element.style.opacity = isDisabled ? "0.5" : "";
      });

      shadowRoot?.querySelectorAll(
        ".c-bXTvXv.c-bXTvXv-lckiv-type-info"
      )?.forEach((button) => {
        button.disabled = isDisabled;
      });
    };

    disableInputs(isDisabled);
  },
};
