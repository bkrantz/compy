class _ModifyMixin:
    _output_funcs = {
        "join": "join_content",
        "update": "update_content",
        "replace": "replace_content",
        "delete": "delete_content"
    }

    def join_content(self, event, content, key_chain, join_key, *args, **kwargs):
        return self.event_join(event=event, content=content, key_chain=key_chain, join_key=join_key)

    def update_content(self, event, content, key_chain, *args, **kwargs):
        return self.event_update(event=event, content=content, key_chain=key_chain)

    def delete_content(self, event, content, key_chain, *args, **kwargs):
        return self.event_delete(event=event, content=content, key_chain=key_chain)

    def replace_content(self, event, content, key_chain, *args, **kwargs):
        return self.event_replace(event=event, content=content, key_chain=key_chain)
